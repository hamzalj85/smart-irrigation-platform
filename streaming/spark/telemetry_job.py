#!/usr/bin/env python3
"""Job Spark Structured Streaming — lecture de la télémétrie d'irrigation.

Étape 4A : lecture Kafka, parsing avec schéma explicite, sortie console.
Les portes qualité arrivent en 4B.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

# ---------------------------------------------------------------------------
# Contrat de données — déclaré, jamais inféré
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


def build_session(app_name: str = "telemetry-job") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        # Tout le pipeline raisonne en UTC. Sans cette ligne, Spark utilise
        # le fuseau de la machine et décale silencieusement les horodatages.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


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


def parse_telemetry(raw: DataFrame) -> DataFrame:
    """Décode le JSON. Fonction pure : testable hors streaming (Phase 10)."""
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


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    parsed = parse_telemetry(read_kafka(spark))

    query = (
        parsed.select("kafka_partition", "kafka_offset", "device_id", "ts",
                      "soil_moisture_pct", "air_temp_c", "air_humidity_pct")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "10")
        .option("checkpointLocation",
                os.getenv("CHECKPOINT_DIR", "/checkpoints") + "/console")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()