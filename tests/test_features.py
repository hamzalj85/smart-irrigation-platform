"""Feature construction and time-based splitting for the model.

The central test in this file is `test_no_leakage_from_the_future`. Data
leakage raises no error: it produces a flattering score and a useless model.
It is the most expensive bug in the discipline, and the only way to guard
against it is to write it down, explicitly, in a test.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from features import (
    FEATURE_COLUMNS,
    IRRIGATION_THRESHOLD_PCT,
    TARGET_COLUMN,
    build_features,
    rule_baseline,
    time_split,
)


def hourly_frame(devices=2, hours=48, start="2026-07-01") -> pd.DataFrame:
    """A synthetic sawtooth hourly table, shaped like fct_readings_hourly."""
    rows = []
    times = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    for d in range(1, devices + 1):
        moisture = 80.0
        for t in times:
            moisture -= 2.0
            if moisture < 30.0:
                moisture = 85.0
            rows.append({
                "device_id": f"esp32-{d:02d}",
                "site_id": "site-a",
                "hour_start": t,
                "reading_count": 12,
                "avg_soil_moisture_pct": moisture,
                "min_soil_moisture_pct": moisture - 1.0,
                "max_soil_moisture_pct": moisture + 1.0,
                "soil_moisture_range_pct": 2.0,
                "avg_air_temp_c": 20.0 + t.hour * 0.1,
                "avg_air_humidity_pct": 60.0,
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Data leakage
# --------------------------------------------------------------------------
def test_no_leakage_from_the_future():
    """Changing the future must change NO explanatory variable.

    We build two datasets identical except for the last hour, then compare the
    features of the preceding hours. If a feature looks forward, it differs and
    the test fails.
    """
    base = hourly_frame(devices=1, hours=48)
    altered = base.copy()
    last = altered.index[-1]
    altered.loc[last, ["avg_soil_moisture_pct", "min_soil_moisture_pct"]] = [5.0, 4.0]

    f_base = build_features(base)
    f_altered = build_features(altered)

    common = min(len(f_base), len(f_altered)) - 1      # exclude the modified row
    pd.testing.assert_frame_equal(
        f_base.loc[: common - 1, FEATURE_COLUMNS],
        f_altered.loc[: common - 1, FEATURE_COLUMNS],
    )


def test_the_target_really_looks_at_the_next_hour():
    frame = hourly_frame(devices=1, hours=48)
    features = build_features(frame, threshold=IRRIGATION_THRESHOLD_PCT)
    merged = features.merge(
        frame[["device_id", "hour_start", "min_soil_moisture_pct"]]
        .assign(hour_start=lambda d: d["hour_start"] - pd.Timedelta(hours=1))
        .rename(columns={"min_soil_moisture_pct": "next_min"}),
        on=["device_id", "hour_start"], how="inner")
    expected = (merged["next_min"] < IRRIGATION_THRESHOLD_PCT).astype(float)
    assert (merged[TARGET_COLUMN] == expected).all()


def test_lags_are_computed_per_device():
    """A global shift would mix the sensors with one another."""
    frame = hourly_frame(devices=3, hours=30)
    features = build_features(frame)
    for _, group in features.groupby("device_id"):
        gap = group["hour_start"].diff().dropna()
        assert (gap == pd.Timedelta(hours=1)).all()


def test_the_hour_is_encoded_on_a_circle():
    """23:00 and 00:00 must be neighbours, not opposites."""
    frame = hourly_frame(devices=1, hours=48)
    features = build_features(frame).set_index("hour_start")
    midnight = features[features.index.hour == 0].iloc[0]
    eleven_pm = features[features.index.hour == 23].iloc[0]
    distance = np.hypot(midnight["hour_sin"] - eleven_pm["hour_sin"],
                        midnight["hour_cos"] - eleven_pm["hour_cos"])
    assert distance < 0.3


def test_no_missing_value_in_the_output():
    features = build_features(hourly_frame(devices=2, hours=48))
    assert not features[FEATURE_COLUMNS].isna().any().any()


# --------------------------------------------------------------------------
# Time-based split
# --------------------------------------------------------------------------
def test_the_split_is_chronological():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert train["hour_start"].max() <= test["hour_start"].min()


def test_no_hour_ends_up_on_both_sides():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert set(train["hour_start"]).isdisjoint(set(test["hour_start"]))


def test_the_proportions_are_respected():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert len(train) + len(test) == len(features)
    assert 0.6 < len(train) / len(features) < 0.8


def test_an_empty_side_fails_loudly():
    """An error beats an empty test set and a meaningless score.

    Four hours of history for a single device leave exactly one usable row once
    the three lags and the target are computed: the test side would be empty.
    `time_split` refuses rather than returning an empty DataFrame, on which the
    metrics would read 0 with nothing to explain why.

    Note: `time_split` only judges the structural validity of the split. Having
    enough volume to train is `train.py`'s responsibility, and it refuses below
    `--min-rows`.
    """
    frame = build_features(hourly_frame(devices=1, hours=4))
    assert len(frame) == 1, "four hours leave only one usable row"
    with pytest.raises(ValueError, match="Empty split"):
        time_split(frame, 0.7)


def test_two_rows_are_structurally_enough():
    """The exact boundary, documented: two rows split into 1 + 1.

    Five hours of history give two rows: three are lost to the 1, 2 and 3 hour
    lags, and one to the target, which looks at the next hour.
    """
    frame = build_features(hourly_frame(devices=1, hours=5))
    assert len(frame) == 2
    train, test = time_split(frame, 0.7)
    assert len(train) == 1
    assert len(test) == 1


@pytest.mark.parametrize("fraction", [0.0, 1.0, -0.5, 1.5])
def test_an_invalid_fraction_is_refused(fraction):
    with pytest.raises(ValueError):
        time_split(build_features(hourly_frame(devices=2, hours=100)), fraction)


# --------------------------------------------------------------------------
# Business baseline
# --------------------------------------------------------------------------
def test_the_business_rule_is_deterministic():
    features = build_features(hourly_frame(devices=2, hours=60))
    assert (rule_baseline(features) == rule_baseline(features)).all()


def test_the_business_rule_only_returns_0_and_1():
    features = build_features(hourly_frame(devices=2, hours=60))
    assert set(rule_baseline(features).unique()) <= {0, 1}
