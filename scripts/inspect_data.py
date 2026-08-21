"""Counts the records produced by the streaming pipeline, read from MinIO.

Reads credentials from .env so there is a single source of truth, and queries
the Parquet files directly over S3 with DuckDB - no Spark needed.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import duckdb


def load_env(path: str = ".env") -> dict[str, str]:
    env_file = Path(path)
    if not env_file.exists():
        sys.exit(f"{path} not found - run this script from the repository root")
    return {
        m.group(1): m.group(2).strip().strip('"').strip("'")
        for m in re.finditer(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$",
                             env_file.read_text(encoding="utf-8"), re.MULTILINE)
    }


def connect(env: dict[str, str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint = 'localhost:9000'")
    con.execute("SET s3_url_style = 'path'")     # MinIO: /bucket/object
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_region = 'us-east-1'")   # required by signature v4
    con.execute("SET s3_access_key_id = ?", [env["MINIO_ROOT_USER"]])
    con.execute("SET s3_secret_access_key = ?", [env["MINIO_ROOT_PASSWORD"]])
    return con


def scalar(con, sql: str, default: int = 0) -> int:
    """No files means 0. Any other error propagates: an authentication
    failure must never look like an empty dataset."""
    try:
        return con.execute(sql).fetchone()[0]
    except duckdb.Error as exc:
        message = str(exc)
        if "No files found" in message:
            return default
        raise RuntimeError(
            f"Cannot read from MinIO: {message.splitlines()[0]}"
        ) from exc


def main() -> int:
    env = load_env()
    bucket = env.get("MINIO_BUCKET", "irrigation")
    clean_glob = f"s3://{bucket}/telemetry/**/*.parquet"
    quar_glob = f"s3://{bucket}/quarantine/**/*.parquet"
    con = connect(env)

    clean = scalar(con, f"""
        SELECT count(*) FROM read_parquet('{clean_glob}', hive_partitioning = true)
    """)

    try:
        rows = con.execute(f"""
            SELECT quality_rule, count(*) AS n
            FROM read_parquet('{quar_glob}', hive_partitioning = true)
            GROUP BY 1 ORDER BY n DESC
        """).fetchall()
    except duckdb.Error as exc:
        if "No files found" not in str(exc):
            raise
        rows = []

    bad = sum(n for _, n in rows)
    total = clean + bad

    print(f"clean       {clean:>7}")
    for rule, n in rows:
        print(f"  {rule:<22} {n:>5}")
    print(f"quarantined {bad:>7}")
    print(f"total       {total:>7}")
    print(f"rejection rate: {bad / total:.1%}" if total else "no data found")

    if clean == 0:
        print("\nNo clean data: has the simulator been running?")
        return 1

    print("\n--- check 1: no out-of-range value in the clean stream ---")
    print(scalar(con, f"""
        SELECT count(*) FROM read_parquet('{clean_glob}', hive_partitioning = true)
        WHERE soil_moisture_pct < 0 OR soil_moisture_pct > 100
           OR soil_moisture_pct IS NULL
    """), "row(s) - must be 0")

    print("\n--- check 2: no duplicate (device_id, ts) ---")
    print(scalar(con, f"""
        SELECT count(*) FROM (
            SELECT device_id, ts
            FROM read_parquet('{clean_glob}', hive_partitioning = true)
            GROUP BY 1, 2 HAVING count(*) > 1
        )
    """), "duplicate key(s) - must be 0")

    print("\n--- breakdown per device ---")
    for dev, n, lo, hi in con.execute(f"""
        SELECT device_id, count(*) AS n,
               round(min(soil_moisture_pct), 1), round(max(soil_moisture_pct), 1)
        FROM read_parquet('{clean_glob}', hive_partitioning = true)
        GROUP BY 1 ORDER BY 1
    """).fetchall():
        print(f"  {dev:<10} {n:>5} rows   moisture {lo}% -> {hi}%")

    print("\n--- partitions written ---")
    for site, date, n in con.execute(f"""
        SELECT site_id, date, count(*)
        FROM read_parquet('{clean_glob}', hive_partitioning = true)
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall():
        print(f"  site_id={site}/date={date}  {n} rows")

    return 0


if __name__ == "__main__":
    sys.exit(main())
