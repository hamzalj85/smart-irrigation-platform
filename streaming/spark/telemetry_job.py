#!/usr/bin/env python3
"""Job Spark Structured Streaming — télémétrie d'irrigation.

Kafka -> schéma explicite -> portes qualité -> déduplication
     -> Parquet propre + Parquet quarantaine.

Les fonctions de transformation sont pures : elles prennent un DataFrame et
en rendent un autre. Elles se testent donc en batch, sans Kafka (Phase 10).
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

from pyspark.sql.streaming import StreamingQueryListener

from datetime import datetime, timezone

from pyspark.sql import Window


class ProgressLogger(StreamingQueryListener):
    """Expose ce que les logs Spark ne montrent pas : les lignes que le
    watermark a jetées. Sans ça, une perte de données est invisible."""

    def onQueryStarted(self, event):
        print(f"[listener] requête démarrée : {event.name}", flush=True)

    def onQueryProgress(self, event):
        p = event.progress
        dropped = sum(getattr(so, "numRowsDroppedByWatermark", 0)
                      for so in (p.stateOperators or []))
        out = getattr(p.sink, "numOutputRows", -1)
        wm = (p.eventTime or {}).get("watermark", "-")
        flag = "  <-- PERTE" if dropped else ""
        print(f"[{p.name}] input={p.numInputRows} output={out} "
              f"droppedByWatermark={dropped} watermark={wm}{flag}", flush=True)
    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[listener] terminée : {event.id}", flush=True)

# ---------------------------------------------------------------------------
# Contrat de données
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
# Règles qualité — les seuils physiques du matériel, pas des nombres ronds
# ---------------------------------------------------------------------------
RANGES: dict[str, tuple[float, float]] = {
    "soil_moisture_pct": (0.0, 100.0),      # pourcentage volumétrique
    "air_temp_c":        (-40.0, 80.0),     # plage du DHT22 élargie
    "air_humidity_pct":  (0.0, 100.0),
    "soil_raw":          (0, 4095),         # ADC 12 bits de l'ESP32
    "battery_v":         (2.5, 5.0),        # LiPo + marge
}
REQUIRED_FIELDS = ["device_id", "site_id", "ts", "soil_moisture_pct"]
MAX_FUTURE_MINUTES = 5
MAX_LATE_HOURS = 24
WATERMARK = "10 minutes"
STATE_FIELDS = ["site_id", "ts", "soil_moisture_pct", "soil_raw",
                "air_temp_c", "air_humidity_pct", "battery_v", "fw_version"]


# ---------------------------------------------------------------------------
# Session et source
# ---------------------------------------------------------------------------
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
            # MinIO n'a pas de DNS par bucket : l'URL est /bucket/objet,
            # pas bucket.host/objet. Sans ceci, chaque requête échoue.
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
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS", "2000"))
        .option("failOnDataLoss", "false")
        .load()
    )


# ---------------------------------------------------------------------------
# Transformations pures
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
    """Ajoute `quality_reason` : NULL si l'enregistrement passe, sinon le motif.

    Les règles sont évaluées dans l'ordre ; la première qui échoue l'emporte.
    """
    unparseable = (F.col("device_id").isNull()
                   & F.col("ts").isNull()
                   & F.col("site_id").isNull())

    missing = F.array_compact(F.array(
        *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_FIELDS]))

    out_of_range = F.array_compact(F.array(
        *[F.when((F.col(c) < lo) | (F.col(c) > hi), F.lit(c))
          for c, (lo, hi) in RANGES.items()]))

    # Référence = l'heure d'ingestion du broker, pas l'horloge murale.
    # Le verdict devient déterministe : rejouer les mêmes données donne
    # exactement les mêmes rejets, quel que soit le moment du traitement.
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
    """Flux valide : dédupliqué sur (device_id, ts), partitionné par site/date."""
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


def quarantined_records(df: DataFrame) -> DataFrame:
    """Flux rejeté : motif + payload original, partitionné par date d'ingestion."""
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
    """Client réutilisé entre micro-lots : ouvrir une connexion toutes les
    15 secondes serait du gaspillage pur."""
    global _mongo_client
    from pymongo import MongoClient
    if _mongo_client is None:
        _mongo_client = MongoClient(os.environ["MONGO_URI"],
                                    serverSelectionTimeoutMS=5000)
    return (_mongo_client[os.getenv("MONGO_DB", "irrigation")]
                         [os.getenv("MONGO_COLLECTION", "device_state")])


def upsert_device_state(batch_df: DataFrame, batch_id: int) -> None:
    """Écrit l'état courant de chaque appareil, sans jamais reculer.

    Deux opérations par appareil :
      1. $setOnInsert crée le document s'il n'existe pas encore ;
      2. $set ne s'applique QUE si le ts stocké est plus ancien.
    Un micro-lot en retard ne peut donc pas écraser un état plus récent.
    """
    from pymongo import UpdateOne

    window = Window.partitionBy("device_id").orderBy(F.col("ts").desc())
    rows = (batch_df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .collect())          # une ligne par appareil : jamais volumineux
    if not rows:
        return

    now = datetime.now(timezone.utc)
    ops = []
    for r in rows:
        doc = {f: r[f] for f in STATE_FIELDS}
        doc["device_id"] = r["device_id"]
        doc["updated_at"] = now
        ops.append(UpdateOne({"_id": r["device_id"]},
                             {"$setOnInsert": doc}, upsert=True))
        ops.append(UpdateOne({"_id": r["device_id"], "ts": {"$lt": r["ts"]}},
                             {"$set": doc}))

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


def start_quality_monitor(df: DataFrame, ckpt: str):
    """Compteur vivant des rejets par règle, affiché dans les logs."""
    return (
        df.groupBy("quality_rule").count()
        .writeStream
        .format("console")
        .queryName("quality-monitor")
        .outputMode("complete")
        .option("truncate", "false")
        .option("checkpointLocation", f"{ckpt}/quality-monitor")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.streams.addListener(ProgressLogger())

    data_dir = os.getenv("DATA_DIR", "/data")
    ckpt = os.getenv("CHECKPOINT_DIR", "/checkpoints")

    checked = apply_quality_gates(parse_telemetry(read_kafka(spark)))
    clean = clean_records(checked)
    bad = quarantined_records(checked)

    start_parquet_sink(clean, "clean", f"{data_dir}/telemetry",
                       ["site_id", "date"], ckpt)
    start_parquet_sink(bad, "quarantine", f"{data_dir}/quarantine",
                       ["quality_rule", "date"], ckpt)
    if os.getenv("MONGO_URI"):
        start_mongo_sink(clean_records(checked), ckpt)
    if os.getenv("QUALITY_MONITOR", "true").lower() == "true":
        start_quality_monitor(bad, ckpt)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()