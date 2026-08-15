"""Nightly retraining -- with a promotion that can refuse to happen.

A retraining job that blindly overwrites a good model with a worse one is a
classic production failure: the pipeline is green, the dashboards are green,
and predictions quietly degrade. Training and promoting are therefore two
distinct steps, and the second one compares before it acts.

Promotion rules, in order:
  1. the candidate must beat the rule-based baseline -- otherwise the model
     has no reason to exist at all;
  2. the candidate must beat the model currently in production on the same
     holdout metric.

If either check fails the DAG still succeeds: refusing to promote is a normal
outcome, not an incident. The refusal and its reason are logged.
"""
from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DUCKDB_PATH = "/opt/airflow/dbt/irrigation.duckdb"
TRAIN_SCRIPT = "/opt/airflow/ml/train.py"
PROMOTION_METRIC = "f1"


@dag(
    dag_id="model_retrain",
    description="Retrains the irrigation classifier and promotes it only if it improves",
    schedule="0 3 * * *",          # apres daily_aggregation (02:00)
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=10)},
    tags=["ml", "batch"],
    doc_md=__doc__,
)
def model_retrain():

    train = BashOperator(
        task_id="train_candidate",
        bash_command=f"python {TRAIN_SCRIPT} --duckdb {DUCKDB_PATH}",
        append_env=True,
    )

    @task
    def promote_if_better() -> dict:
        import json
        import logging
        import os

        import boto3

        log = logging.getLogger(__name__)
        bucket = os.getenv("MINIO_BUCKET", "irrigation")
        client = boto3.client(
            "s3",
            endpoint_url="http://" + os.getenv("S3_ENDPOINT_HOST", "minio:9000"),
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )

        def read_json(key: str) -> dict | None:
            try:
                return json.loads(
                    client.get_object(Bucket=bucket, Key=key)["Body"].read())
            except client.exceptions.NoSuchKey:
                return None

        # Les versions sont horodatees : l'ordre lexicographique est l'ordre
        # chronologique. Le dernier prefixe est donc le candidat.
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix="models/", Delimiter="/")
        versions = sorted(
            p["Prefix"].split("/")[1]
            for page in pages for p in page.get("CommonPrefixes", [])
            if p["Prefix"].split("/")[1] != "current"
        )
        if not versions:
            raise RuntimeError(f"Aucun modele candidat dans s3://{bucket}/models/")
        candidate_version = versions[-1]

        candidate = read_json(f"models/{candidate_version}/metrics.json")
        current = read_json("models/current/metrics.json")

        selected = candidate["selected_model"]
        candidate_score = candidate["results"][selected][PROMOTION_METRIC]
        baseline_score = candidate["results"]["baseline_rule"][PROMOTION_METRIC]

        decision = {
            "candidate_version": candidate_version,
            "candidate_model": selected,
            "candidate_score": candidate_score,
            "baseline_score": baseline_score,
            "current_version": current["version"] if current else None,
            "current_score": None,
            "promoted": False,
            "reason": "",
        }

        if candidate_score <= baseline_score:
            decision["reason"] = (
                f"candidate {PROMOTION_METRIC}={candidate_score} does not beat "
                f"the rule baseline ({baseline_score})")
            log.warning("Promotion refusee : %s", decision["reason"])
            return decision

        if current is not None:
            current_selected = current["selected_model"]
            current_score = current["results"][current_selected][PROMOTION_METRIC]
            decision["current_score"] = current_score
            if candidate_score <= current_score:
                decision["reason"] = (
                    f"candidate {PROMOTION_METRIC}={candidate_score} does not beat "
                    f"production ({current_score}, version {current['version']})")
                log.warning("Promotion refusee : %s", decision["reason"])
                return decision

        for name in ("model.joblib", "metrics.json"):
            client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket,
                            "Key": f"models/{candidate_version}/{name}"},
                Key=f"models/current/{name}")

        decision["promoted"] = True
        decision["reason"] = (
            f"{PROMOTION_METRIC} {decision['current_score']} -> {candidate_score}"
            if decision["current_score"] is not None
            else "first model promoted")
        log.info("Promotion : %s", decision["reason"])
        return decision

    train >> promote_if_better()


model_retrain()
