#!/usr/bin/env python3
"""Spark Structured Streaming job -- irrigation telemetry.

Kafka -> explicit schema -> quality gates -> deduplication
     -> clean Parquet + quarantine Parquet.

The transformation functions are pure: they take a DataFrame and return
another one. They can therefore be tested in batch mode, without Kafka.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from prometheus_client import Counter as PromCounter
from prometheus_client import Gauge, start_http_server

try:
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.streaming import StreamingQueryListener
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
except ImportError:  # pragma: no cover - exercised in environments without PySpark
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    Window = Any  # type: ignore[assignment]
    F = Any  # type: ignore[assignment]
    StreamingQueryListener = object  # type: ignore[assignment]
    DoubleType = IntegerType = StringType = StructField = StructType = Any  # type: ignore[assignment]



# ---------------------------------------------------------------------------
# Data contract
# ---------------------------------------------------------------------------
TELEMETRY_SCHEMA = StructType([
    StructField("device_id",         StringType(),  True),
    StructField("site_id",           StringType(),  True),
    StructField("ts",                StringType(),  True),
    StructField("soil_moisture_pct", DoubleType(),  True),
    StructField("soil_raw",          IntegerType(), True),
    StructField("air_temp_c",        DoubleType(),  True),
    StructField("air_humidity_pct",  DoubleType(),  True),
    StructField("battery_v",         DoubleType(),  True),
    StructField("fw_version",        StringType(),  True),
])

# ---------------------------------------------------------------------------
# Quality rules -- the physical limits of the hardware, not round numbers
# ---------------------------------------------------------------------------
RANGES: dict[str, tuple[float, float]] = {
    "soil_moisture_pct": (0.0, 100.0),      # volumetric percentage
    "air_temp_c":        (-40.0, 80.0),     # DHT22 datasheet range, widened
    "air_humidity_pct":  (0.0, 100.0),
    "soil_raw":          (0, 4095),         # 12-bit ESP32 ADC
    "battery_v":         (2.5, 5.0),        # LiPo range plus margin
}
REQUIRED_FIELDS = ["device_id", "site_id", "ts", "soil_moisture_pct"]
MAX_FUTURE_MINUTES = 5
MAX_LATE_HOURS = 24
WATERMARK = "10 minutes"
# ---------------------------------------------------------------------------
# Prometheus metrics
#
# What Spark does not expose by itself: the rows discarded by the watermark,
# the watermark lag, and the breakdown of quality rejections. Those are exactly
# the numbers you want on a dashboard.
# ---------------------------------------------------------------------------
QUERY_INPUT_ROWS = PromCounter(
    "irrigation_spark_input_rows_total",
    "Rows read from Kafka, per streaming query", ["query"],
)
QUERY_OUTPUT_ROWS = PromCounter(
    "irrigation_spark_output_rows_total",
    "Rows written by the sink, per streaming query", ["query"],
)
WATERMARK_DROPPED = PromCounter(
    "irrigation_spark_rows_dropped_by_watermark_total",
    "Rows that arrived too late and were dropped by a stateful operator", ["query"],
)
BATCH_DURATION = Gauge(
    "irrigation_spark_batch_duration_seconds",
    "Duration of the last micro-batch", ["query"],
)
QUARANTINE = PromCounter(
    "irrigation_spark_quarantined_rows_total",
    "Rows sent to quarantine, per quality rule", ["rule"],
)
CLEAN_ROWS = PromCounter(
    "irrigation_spark_clean_rows_total",
    "Rows that passed the three quality gates",
)
DEVICE_MOISTURE = Gauge(
    "irrigation_device_soil_moisture_pct",
    "Last known soil moisture", ["device_id", "site_id"],
)
DEVICE_BATTERY = Gauge(
    "irrigation_device_battery_volts",
    "Last known battery voltage", ["device_id", "site_id"],
)
DEVICE_LAST_SEEN = Gauge(
    "irrigation_device_last_reading_timestamp_seconds",
    "Timestamp of the last valid reading (epoch seconds)", ["device_id", "site_id"],
)

STATE_FIELDS = ["site_id", "ts", "soil_moisture_pct", "soil_raw",
                "air_temp_c", "air_humidity_pct", "battery_v", "fw_version"]


# ---------------------------------------------------------------------------
# Session and source
# ---------------------------------------------------------------------------

class ProgressLogger(StreamingQueryListener):
    """Exposes what the Spark logs do not show: the rows the watermark threw
    away. Without this, a data loss is invisible by design."""

    def onQueryStarted(self, event):
        print(f"[listener] query started: {event.name}", flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        dropped = sum(getattr(so, "numRowsDroppedByWatermark", 0)
                      for so in (p.stateOperators or []))
        out = getattr(p.sink, "numOutputRows", -1)
        wm = (p.eventTime or {}).get("watermark", "-")
        QUERY_INPUT_ROWS.labels(query=p.name).inc(p.numInputRows)
        if out and out > 0:
            QUERY_OUTPUT_ROWS.labels(query=p.name).inc(out)
        if dropped:
            WATERMARK_DROPPED.labels(query=p.name).inc(dropped)
        BATCH_DURATION.labels(query=p.name).set(p.batchDuration / 1000.0)

        flag = "  <-- LOSS" if dropped else ""
        print(f"[{p.name}] input={p.numInputRows} output={out} "
              f"droppedByWatermark={dropped} watermark={wm}{flag}", flush=True)
    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[listener] terminated: {event.id}", flush=True)

def build_session(app_name: str = "telemetry-job") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.streaming.metricsEnabled", "true")
    )

    endpoint = os.getenv("S3_ENDPOINT")
    if endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
            # MinIO has no per-bucket DNS: the URL is /bucket/object, not
            # bucket.host/object. Without this, every request fails.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )
    return builder.getOrCreate()


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"))
        .option("subscribe", os.getenv("KAFKA_TELEMETRY_TOPIC",
                                       "irrigation.telemetry"))
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS", "2000"))
        .option("failOnDataLoss", "false")
        .load()
    )


# ---------------------------------------------------------------------------
# Pure transformations
# ---------------------------------------------------------------------------
def parse_telemetry(raw: DataFrame) -> DataFrame:
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw_value"),
        )
        .withColumn("d", F.from_json(F.col("raw_value"), TELEMETRY_SCHEMA))
        .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_ts",
                "raw_value", "d.*")
        .withColumn("ts", F.to_timestamp(F.col("ts")))
    )


def apply_quality_gates(df: DataFrame) -> DataFrame:
    """Adds `quality_reason`: NULL when the record passes, the reason otherwise.

    The rules are evaluated in order; the first one that fails wins.
    """
    unparseable = (F.col("device_id").isNull()
                   & F.col("ts").isNull()
                   & F.col("site_id").isNull())

    missing = F.array_compact(F.array(
        *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_FIELDS]))

    out_of_range = F.array_compact(F.array(
        *[F.when((F.col(c) < lo) | (F.col(c) > hi), F.lit(c))
          for c, (lo, hi) in RANGES.items()]))

    # Reference = the broker ingestion time, never the wall clock.
    # This makes the verdict deterministic: replaying the same data produces
    # exactly the same rejections, whenever the processing happens.
    future = F.col("ts") > F.col("kafka_ts") + F.expr(
        f"INTERVAL {MAX_FUTURE_MINUTES} MINUTES")
    too_old = F.col("ts") < F.col("kafka_ts") - F.expr(
        f"INTERVAL {MAX_LATE_HOURS} HOURS")

    return df.withColumn(
        "quality_reason",
        F.when(unparseable, F.lit("unparseable_payload"))
         .when(F.size(missing) > 0,
               F.concat(F.lit("completeness:"), F.array_join(missing, ",")))
         .when(F.size(out_of_range) > 0,
               F.concat(F.lit("range:"), F.array_join(out_of_range, ",")))
         .when(future, F.lit("plausibility:future_ts"))
         .when(too_old, F.lit("plausibility:stale_ts"))
         .otherwise(F.lit(None).cast("string"))
    )


def clean_records(df: DataFrame) -> DataFrame:
    """Valid stream: deduplicated on (device_id, ts), partitioned by site/date."""
    return (
        df.filter(F.col("quality_reason").isNull())
        .withWatermark("ts", WATERMARK)
        .dropDuplicates(["device_id", "ts"])
        .withColumn("date", F.to_date(F.col("ts")))
        .select("device_id", "site_id", "ts", "date",
                "soil_moisture_pct", "soil_raw", "air_temp_c",
                "air_humidity_pct", "battery_v", "fw_version",
                "kafka_partition", "kafka_offset")
    )

def device_state_stream(df: DataFrame) -> DataFrame:
    """Stream headed for MongoDB: no watermark, no deduplication.

    The conditional upsert on the Mongo side is already idempotent and order
    insensitive. A stateful operator here would be a free point of fragility.
    """
    return df.filter(F.col("quality_reason").isNull())

def quarantined_records(df: DataFrame) -> DataFrame:
    """Rejected stream: reason + original payload, partitioned by ingestion date."""
    return (
        df.filter(F.col("quality_reason").isNotNull())
        .withColumn("quality_rule",
                    F.split(F.col("quality_reason"), ":").getItem(0))
        .withColumn("date", F.to_date(F.col("kafka_ts")))
        .select("quality_rule", "quality_reason", "device_id", "site_id",
                "ts", "date", "kafka_ts", "kafka_partition", "kafka_offset",
                "raw_value")
    )

_mongo_client = None


def _device_state_collection():
    """Client reused across micro-batches: opening a connection every 15
    seconds would be pure waste."""
    global _mongo_client
    from pymongo import MongoClient
    if _mongo_client is None:
        _mongo_client = MongoClient(os.environ["MONGO_URI"],
                                    serverSelectionTimeoutMS=5000)
    return (_mongo_client[os.getenv("MONGO_DB", "irrigation")]
                         [os.getenv("MONGO_COLLECTION", "device_state")])


def upsert_device_state(batch_df: DataFrame, batch_id: int) -> None:
    """Writes the current state of each device, and never moves it backwards.

    Two operations per device:
      1. $setOnInsert creates the document when it does not exist yet;
      2. $set applies ONLY when the stored ts is older.
    A late micro-batch therefore cannot overwrite a more recent state.
    """
    from pymongo import UpdateOne

    window = Window.partitionBy("device_id").orderBy(F.col("ts").desc())
    rows = (batch_df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .collect())          # one row per device: never large
    if not rows:
        return

    now = datetime.now(UTC)
    ops = []
    for r in rows:
        doc = {f: r[f] for f in STATE_FIELDS}
        doc["device_id"] = r["device_id"]
        doc["updated_at"] = now
        ops.append(UpdateOne({"_id": r["device_id"]},
                             {"$setOnInsert": doc}, upsert=True))
        ops.append(UpdateOne({"_id": r["device_id"], "ts": {"$lt": r["ts"]}},
                             {"$set": doc}))

    for r in rows:
        labels = {"device_id": r["device_id"], "site_id": r["site_id"]}
        if r["soil_moisture_pct"] is not None:
            DEVICE_MOISTURE.labels(**labels).set(r["soil_moisture_pct"])
        if r["battery_v"] is not None:
            DEVICE_BATTERY.labels(**labels).set(r["battery_v"])
        DEVICE_LAST_SEEN.labels(**labels).set(r["ts"].timestamp())
    CLEAN_ROWS.inc(batch_df.count())

    result = _device_state_collection().bulk_write(ops, ordered=True)
    print(f"[device-state] batch={batch_id} devices={len(rows)} "
          f"upserted={result.upserted_count} modified={result.modified_count}",
          flush=True)


def start_mongo_sink(df: DataFrame, ckpt: str):
    return (
        df.writeStream
        .queryName("device-state")
        .outputMode("append")
        .foreachBatch(upsert_device_state)
        .option("checkpointLocation", f"{ckpt}/device-state")
        .trigger(processingTime="15 seconds")
        .start()
    )

# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
def start_parquet_sink(df: DataFrame, name: str, path: str,
                       partitions: list[str], ckpt: str):
    return (
        df.writeStream
        .format("parquet")
        .queryName(name)
        .outputMode("append")
        .partitionBy(*partitions)
        .option("path", path)
        .option("checkpointLocation", f"{ckpt}/{name}")
        .trigger(processingTime="30 seconds")
        .start()
    )


def count_quarantine(batch_df: DataFrame, batch_id: int) -> None:
    """Counts rejections per rule and feeds Prometheus.

    This replaces the former console sink in `complete` mode, which printed a
    running total since startup -- a figure you cannot differentiate into a
    rate. A Prometheus counter can be, with rate(), over any window you like.
    """
    rows = batch_df.groupBy("quality_rule").count().collect()
    if not rows:
        return
    for row in rows:
        QUARANTINE.labels(rule=row["quality_rule"]).inc(row["count"])
    detail = " ".join(f"{r['quality_rule']}={r['count']}" for r in rows)
    print(f"[quality-monitor] batch={batch_id} {detail}", flush=True)


def start_quality_monitor(df: DataFrame, ckpt: str):
    return (
        df.writeStream
        .queryName("quality-monitor")
        .outputMode("append")
        .foreachBatch(count_quarantine)
        .option("checkpointLocation", f"{ckpt}/quality-monitor")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.streams.addListener(ProgressLogger())
    start_http_server(int(os.getenv("METRICS_PORT", "8000")))
    print(f"[metrics] Prometheus on :{os.getenv('METRICS_PORT', '8000')}/metrics",
          flush=True)

    data_dir = os.getenv("DATA_DIR", "/data").rstrip("/")
    ckpt = os.getenv("CHECKPOINT_DIR", "/checkpoints").rstrip("/")

    checked = apply_quality_gates(parse_telemetry(read_kafka(spark)))
    clean = clean_records(checked)
    bad = quarantined_records(checked)

    start_parquet_sink(clean, "clean", f"{data_dir}/telemetry",
                       ["site_id", "date"], ckpt)
    start_parquet_sink(bad, "quarantine", f"{data_dir}/quarantine",
                       ["quality_rule", "date"], ckpt)
    if os.getenv("MONGO_URI"):
        start_mongo_sink(device_state_stream(checked), ckpt)
    if os.getenv("QUALITY_MONITOR", "true").lower() == "true":
        start_quality_monitor(bad, ckpt)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
