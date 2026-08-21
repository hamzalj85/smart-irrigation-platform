"""The three quality gates of the streaming layer.

These tests need PySpark and a JVM, but **not** Kafka, MinIO or MongoDB:
`apply_quality_gates` takes a DataFrame and returns another one. The streaming
job is only wiring around that pure function.

Marked `spark`: `pytest -m "not spark"` excludes them on a machine with no JVM.
"""
from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyspark", reason="PySpark is not installed")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from telemetry_job import (
    MAX_LATE_HOURS,
    TELEMETRY_SCHEMA,
    apply_quality_gates,
    parse_telemetry,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[1]")
               .appName("tests-quality-gates")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def kafka_frame(spark, payloads, ingested_at=None):
    """Mimics the shape of a DataFrame read from the Kafka connector."""
    ingested_at = ingested_at or dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
    rows = [(name, text) for name, text in payloads]
    return (spark.createDataFrame(rows, "case string, value string")
            .withColumn("key", F.lit("k").cast("binary"))
            .withColumn("partition", F.lit(0))
            .withColumn("offset", F.lit(0).cast("long"))
            .withColumn("timestamp", F.lit(ingested_at).cast("timestamp"))
            .withColumn("value", F.col("value").cast("binary")))


def verdicts(spark, payloads, ingested_at=None) -> dict[str, str | None]:
    """Returns {case label: rejection reason}, one pass per case."""
    out: dict[str, str | None] = {}
    for name, text in payloads:
        frame = kafka_frame(spark, [(name, text)], ingested_at)
        row = apply_quality_gates(parse_telemetry(frame)).collect()[0]
        out[name] = row["quality_reason"]
    return out


def message(**overrides) -> str:
    import json
    base = {
        "device_id": "esp32-01", "site_id": "site-a",
        "ts": "2026-08-10T11:59:00.000Z",
        "soil_moisture_pct": 54.89, "soil_raw": 2102,
        "air_temp_c": 16.64, "air_humidity_pct": 68.76,
        "battery_v": 4.1, "fw_version": "1.0.0",
    }
    base.update(overrides)
    return json.dumps({k: v for k, v in base.items() if v is not ...})


def test_a_valid_message(spark):
    assert verdicts(spark, [("ok", message())])["ok"] is None


def test_completeness_gate(spark):
    cases = verdicts(spark, [
        ("null_moisture", message(soil_moisture_pct=None)),
        ("no_ts", message(ts=...)),
    ])
    assert cases["null_moisture"] == "completeness:soil_moisture_pct"
    assert cases["no_ts"].startswith("completeness:")


@pytest.mark.parametrize(("field", "value"), [
    ("soil_moisture_pct", 148.0),
    ("soil_moisture_pct", -12.4),
    ("air_temp_c", -273.0),
    ("air_humidity_pct", 120.0),
    ("soil_raw", 5000),          # beyond the 12 bits of the ADC
    ("battery_v", 9.9),
])
def test_range_gate(spark, field, value):
    reason = verdicts(spark, [("x", message(**{field: value}))])["x"]
    assert reason == f"range:{field}"


def test_plausibility_gate_timestamp_too_old(spark):
    old = (dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
           - dt.timedelta(hours=MAX_LATE_HOURS + 2))
    reason = verdicts(spark, [("x", message(ts=old.isoformat()))])["x"]
    assert reason == "plausibility:stale_ts"


def test_plausibility_gate_timestamp_in_the_future(spark):
    future = (dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
              + dt.timedelta(hours=2))
    reason = verdicts(spark, [("x", message(ts=future.isoformat()))])["x"]
    assert reason == "plausibility:future_ts"


def test_unreadable_payload(spark):
    assert verdicts(spark, [("x", "this is not json at all")])["x"] == \
        "unparseable_payload"


def test_the_verdict_is_deterministic(spark):
    """Same data, same ingestion time, same verdict -- always.

    Plausibility is anchored on `kafka_ts`, never on the wall clock. Replaying
    a history one month later must produce exactly the same rejections,
    otherwise the pipeline is not replayable.
    """
    old = dt.datetime(2024, 1, 1, 8, 0, tzinfo=dt.UTC)
    payload = message(ts=old.isoformat())
    ingestion = old + dt.timedelta(minutes=1)
    assert verdicts(spark, [("x", payload)], ingestion)["x"] is None


def test_the_first_failing_rule_wins(spark):
    """Order: completeness, then range, then plausibility."""
    reason = verdicts(spark, [("x", message(soil_moisture_pct=None,
                                            air_temp_c=999.0))])["x"]
    assert reason == "completeness:soil_moisture_pct"


def test_the_schema_is_declared_not_inferred():
    """The data contract is explicit: nine typed columns."""
    assert len(TELEMETRY_SCHEMA.fields) == 9
    types = {f.name: f.dataType.simpleString() for f in TELEMETRY_SCHEMA.fields}
    assert types["soil_moisture_pct"] == "double"
    assert types["soil_raw"] == "int"
    assert types["ts"] == "string"   # converted later, to control the failure
