#!/usr/bin/env python3
"""Trains the irrigation-need classifier on the dbt marts.

The model is a **downstream consumer** of the platform, not its centrepiece:
it reads `fct_readings_hourly`, the same table an analyst would query. Nothing
here re-implements cleaning or aggregation -- that work belongs upstream.

Every run produces a versioned artefact and a metrics file in MinIO. Nothing is
promoted automatically: promotion is a separate, conditional decision handled by
the `model_retrain` DAG.

    python ml/train.py --duckdb transforms/dbt/irrigation.duckdb
"""
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import duckdb
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import (
    FEATURE_COLUMNS,
    IRRIGATION_THRESHOLD_PCT,
    TARGET_COLUMN,
    build_features,
    rule_baseline,
    time_split,
)


def load_hourly(duckdb_path: str) -> pd.DataFrame:
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        return con.execute("""
            SELECT device_id, site_id, hour_start, reading_count,
                   avg_soil_moisture_pct, min_soil_moisture_pct,
                   max_soil_moisture_pct, soil_moisture_range_pct,
                   avg_air_temp_c, avg_air_humidity_pct
            FROM fct_readings_hourly
            ORDER BY device_id, hour_start
        """).df()
    finally:
        con.close()


def evaluate(y_true, y_pred, y_proba=None) -> dict:
    scores = {
        "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
    }
    if y_proba is not None and len(set(y_true)) > 1:
        scores["roc_auc"] = round(float(roc_auc_score(y_true, y_proba)), 4)
    return scores


def git_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, text=True).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url="http://" + os.getenv("S3_ENDPOINT_HOST", "localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duckdb", default="transforms/dbt/irrigation.duckdb")
    p.add_argument("--train-fraction", type=float, default=0.7)
    p.add_argument("--threshold", type=float, default=IRRIGATION_THRESHOLD_PCT)
    p.add_argument("--min-rows", type=int, default=500,
                   help="refuse d'entrainer en dessous de ce volume")
    p.add_argument("--no-upload", action="store_true")
    args = p.parse_args()

    hourly = load_hourly(args.duckdb)
    data = build_features(hourly, threshold=args.threshold)

    if len(data) < args.min_rows:
        print(f"ECHEC : {len(data)} lignes exploitables, minimum {args.min_rows}.\n"
              f"Genere un historique : python ml/generate_history.py --days 45",
              file=sys.stderr)
        return 2

    train, test = time_split(data, args.train_fraction)
    x_train, y_train = train[FEATURE_COLUMNS], train[TARGET_COLUMN].astype(int)
    x_test, y_test = test[FEATURE_COLUMNS], test[TARGET_COLUMN].astype(int)

    # --- Les chiffres que le memoire d'origine omettait ---------------------
    dataset = {
        "rows_total": len(data),
        "rows_train": len(train),
        "rows_test": len(test),
        "devices": int(data["device_id"].nunique()),
        "positive_rate_overall": round(float(data[TARGET_COLUMN].mean()), 4),
        "positive_rate_train": round(float(y_train.mean()), 4),
        "positive_rate_test": round(float(y_test.mean()), 4),
        "train_period": [str(train["hour_start"].min()), str(train["hour_start"].max())],
        "test_period": [str(test["hour_start"].min()), str(test["hour_start"].max())],
        "split_method": "chronological, no shuffle",
        "threshold_pct": args.threshold,
    }

    results = {"baseline_rule": evaluate(y_test, rule_baseline(x_test, args.threshold))}

    logreg = Pipeline([
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ]).fit(x_train, y_train)
    results["logistic_regression"] = evaluate(
        y_test, logreg.predict(x_test), logreg.predict_proba(x_test)[:, 1])

    forest = RandomForestClassifier(
        n_estimators=200, min_samples_leaf=5, class_weight="balanced",
        random_state=42, n_jobs=-1).fit(x_train, y_train)
    results["random_forest"] = evaluate(
        y_test, forest.predict(x_test), forest.predict_proba(x_test)[:, 1])

    best_name = max(("logistic_regression", "random_forest"),
                    key=lambda n: results[n]["f1"])
    best_model = logreg if best_name == "logistic_regression" else forest

    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    metrics = {
        "version": version,
        "git_revision": git_revision(),
        "trained_at": datetime.now(UTC).isoformat(),
        "features": FEATURE_COLUMNS,
        "target": TARGET_COLUMN,
        "dataset": dataset,
        "results": results,
        "selected_model": best_name,
        "selection_metric": "f1",
        "feature_importances": dict(sorted(
            zip(FEATURE_COLUMNS,
                (round(float(v), 4) for v in forest.feature_importances_),
                strict=True),
            key=lambda kv: -kv[1])),
    }

    print(json.dumps({"dataset": dataset, "results": results,
                      "selected": best_name}, indent=2))

    if args.no_upload:
        print("--no-upload : rien n'a ete envoye dans MinIO")
        return 0

    bucket = os.getenv("MINIO_BUCKET", "irrigation")
    client = s3_client()
    buffer = io.BytesIO()
    joblib.dump({"model": best_model, "features": FEATURE_COLUMNS,
                 "threshold": args.threshold}, buffer)
    client.put_object(Bucket=bucket, Key=f"models/{version}/model.joblib",
                      Body=buffer.getvalue())
    client.put_object(Bucket=bucket, Key=f"models/{version}/metrics.json",
                      Body=json.dumps(metrics, indent=2).encode(),
                      ContentType="application/json")
    print(f"artefact publie : s3://{bucket}/models/{version}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
