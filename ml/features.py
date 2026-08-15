"""Feature engineering for the irrigation-need classifier.

Pure functions: they take a DataFrame and return a DataFrame. No I/O, no
globals, no side effects -- so they can be unit-tested without MinIO, DuckDB
or Airflow (Phase 10).

The single rule that governs this module: **a feature may only use information
available at the moment of prediction.** Every lag is backwards, the target is
the only thing that looks forward. Breaking that rule is the most common way to
build a model that scores beautifully and is useless in production.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Business rule, not a device setting: below this the crop is considered at
# risk. Each simulated device has its own trigger between 28 and 35 %, so the
# model cannot simply memorise one threshold.
IRRIGATION_THRESHOLD_PCT = 32.0

FEATURE_COLUMNS = [
    "avg_soil_moisture_pct",
    "min_soil_moisture_pct",
    "max_soil_moisture_pct",
    "soil_moisture_range_pct",
    "avg_air_temp_c",
    "avg_air_humidity_pct",
    "avg_soil_lag1",
    "avg_soil_lag2",
    "avg_soil_lag3",
    "min_soil_lag1",
    "min_soil_lag2",
    "min_soil_lag3",
    "delta_1h",
    "delta_3h",
    "hour_sin",
    "hour_cos",
    "reading_count",
]
TARGET_COLUMN = "needs_irrigation_next_hour"


def build_features(hourly: pd.DataFrame,
                   threshold: float = IRRIGATION_THRESHOLD_PCT) -> pd.DataFrame:
    """Turns fct_readings_hourly into a supervised learning table.

    Target: will the *minimum* soil moisture of the NEXT hour fall below the
    threshold? Minimum and not average, because a crop suffers from the worst
    moment of the hour, not from its mean.
    """
    df = hourly.sort_values(["device_id", "hour_start"]).copy()
    df["hour_start"] = pd.to_datetime(df["hour_start"], utc=True)
    grouped = df.groupby("device_id", sort=False)

    for lag in (1, 2, 3):
        df[f"avg_soil_lag{lag}"] = grouped["avg_soil_moisture_pct"].shift(lag)
        df[f"min_soil_lag{lag}"] = grouped["min_soil_moisture_pct"].shift(lag)

    # Drying speed: the physical driver of the phenomenon.
    df["delta_1h"] = df["avg_soil_moisture_pct"] - df["avg_soil_lag1"]
    df["delta_3h"] = df["avg_soil_moisture_pct"] - df["avg_soil_lag3"]

    # Hour of day encoded on a circle: 23 h and 00 h must be neighbours.
    hours = df["hour_start"].dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * hours / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hours / 24)

    # The only forward-looking column.
    df[TARGET_COLUMN] = (
        grouped["min_soil_moisture_pct"].shift(-1) < threshold
    ).astype("float")

    keep = ["device_id", "site_id", "hour_start", *FEATURE_COLUMNS, TARGET_COLUMN]
    return df[keep].dropna().reset_index(drop=True)


def time_split(features: pd.DataFrame, train_fraction: float = 0.7
               ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits by TIME, never randomly.

    A random split on a time series puts hour t in train and hour t+1 in test.
    Soil moisture being strongly autocorrelated, the model then "predicts" a
    value it has almost already seen -- accuracy inflates and the score means
    nothing. Splitting on a date reproduces the real question: having learned
    from the past, what does the model do on a future it has never seen?
    """
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be strictly between 0 and 1")
    cutoff = features["hour_start"].quantile(train_fraction)
    train = features[features["hour_start"] <= cutoff]
    test = features[features["hour_start"] > cutoff]
    if train.empty or test.empty:
        raise ValueError(
            f"Empty split at {cutoff}: not enough history to train "
            f"({len(features)} rows)."
        )
    return train.reset_index(drop=True), test.reset_index(drop=True)


def rule_baseline(features: pd.DataFrame,
                  threshold: float = IRRIGATION_THRESHOLD_PCT,
                  margin: float = 2.0) -> pd.Series:
    """The business rule a model must beat to justify its existence.

    "If the soil is already close to the threshold, it will cross it next
    hour." Any model that does not clearly beat this deserves to be replaced
    by four lines of SQL.
    """
    return (features["min_soil_moisture_pct"] < threshold + margin).astype(int)
