#!/usr/bin/env python3
"""Backfills a synthetic history into the lake, for model training.

Why this exists: the streaming pipeline produces data in real time, so getting
several weeks of history would mean leaving the simulator running for several
weeks. This script replays the *same physical model* -- it imports `Device`
from the simulator, so there is a single source of truth for the physics --
and writes the result straight to MinIO in the layout the Spark job uses.

This is a development shortcut, and it must be stated as such in the README:
the streaming path is the production one, this is a backfill for training.

    python ml/generate_history.py --days 45 --sensors 6 --interval 5m
"""
from __future__ import annotations

import argparse
import math
import os
import random
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

# Single source of truth for the soil physics: the simulator module.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "simulator"))
from simulator import ADC_DRY, ADC_WET, FW_VERSION, Device, parse_duration


def build_history(days: int, sensors: int, interval_s: float, site_id: str,
                  seed: int, end: datetime) -> pd.DataFrame:
    rng = random.Random(seed)
    devices = [Device.create(i + 1, site_id, rng) for i in range(sensors)]
    steps = int(days * 24 * 3600 / interval_s)
    start = end - timedelta(seconds=steps * interval_s)

    records = []
    for step in range(steps):
        now = start + timedelta(seconds=step * interval_s)
        hour = now.hour + now.minute / 60.0
        temp = 19.0 + 7.5 * math.sin(2 * math.pi * (hour - 9.0) / 24.0)
        temp += rng.gauss(0.0, 0.4)
        humidity = max(20.0, min(95.0, 95.0 - 1.6 * temp + rng.gauss(0.0, 2.0)))

        for device in devices:
            device.step(interval_s, rng)
            measured = device.moisture + device.calibration_bias
            measured = max(0.0, min(100.0, measured))
            records.append({
                "device_id": device.device_id,
                "site_id": device.site_id,
                "ts": now,
                "date": now.date(),
                "soil_moisture_pct": round(measured, 2),
                "soil_raw": int(ADC_DRY - (measured / 100.0) * (ADC_DRY - ADC_WET)),
                "air_temp_c": round(temp, 2),
                "air_humidity_pct": round(humidity, 2),
                "battery_v": round(device.battery, 3),
                "fw_version": FW_VERSION,
            })
    return pd.DataFrame.from_records(records)


def write_to_lake(frame: pd.DataFrame, bucket: str, prefix: str,
                  endpoint: str, access_key: str, secret_key: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint = ?", [endpoint])
    con.execute("SET s3_url_style = 'path'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_region = 'us-east-1'")
    con.execute("SET s3_access_key_id = ?", [access_key])
    con.execute("SET s3_secret_access_key = ?", [secret_key])
    con.register("history", frame)
    con.execute(f"""
        COPY (SELECT * FROM history)
        TO 's3://{bucket}/{prefix}'
        (FORMAT PARQUET, PARTITION_BY (site_id, date),
         OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'backfill-{{uuid}}')
    """)
    con.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=45)
    p.add_argument("--sensors", type=int, default=6)
    p.add_argument("--interval", type=parse_duration, default="5m",
                   help="delay between two simulated readings (e.g. 5m)")
    p.add_argument("--site-id", default="site-a")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--prefix", default="telemetry")
    p.add_argument("--dry-run", action="store_true",
                   help="generate and summarise without writing to MinIO")
    args = p.parse_args()

    # The history stops at midnight last night: we must not overwrite the
    # partitions the real-time pipeline is filling today.
    end = datetime.now(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0)

    frame = build_history(args.days, args.sensors, args.interval,
                          args.site_id, args.seed, end)

    print(f"{len(frame):,} readings generated")
    print(f"  from {frame['ts'].min()} to {frame['ts'].max()}")
    print(f"  {frame['device_id'].nunique()} appareils, "
          f"{frame['date'].nunique()} days")
    print(f"  moisture: {frame['soil_moisture_pct'].min():.1f} % -> "
          f"{frame['soil_moisture_pct'].max():.1f} %")

    if args.dry_run:
        print("--dry-run: nothing was written")
        return 0

    bucket = os.getenv("MINIO_BUCKET", "irrigation")
    write_to_lake(
        frame, bucket, args.prefix,
        os.getenv("S3_ENDPOINT_HOST", "localhost:9000"),
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
    )
    print(f"written to s3://{bucket}/{args.prefix}/site_id=.../date=...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
