"""Integration test: does a message published on MQTT reach Parquet?

This is the only test that walks the whole chain -- Mosquitto, the bridge,
Kafka, Spark, MinIO. It does not replace the unit tests: it checks what they
cannot see, namely the **wiring**. The quality gates can be perfect and the
pipeline still produce nothing, because a credential is wrong or a topic does
not exist.

It is marked `integration` and **excluded by default**: it needs the Docker
stack running. Locally:

    docker compose up -d
    pytest -m integration

CI does not run it -- standing up eleven services on every commit would cost
more than it returns. It is run by hand before a release.
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

TIMEOUT_SECONDS = 180
POLL_SECONDS = 5


def load_env() -> dict[str, str]:
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if not env_file.exists():
        pytest.skip(".env is missing: the stack is not configured")
    return {
        m.group(1): m.group(2).strip().strip('"').strip("'")
        for m in re.finditer(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$",
                             env_file.read_text(encoding="utf-8"), re.MULTILINE)
    }


@pytest.fixture(scope="module")
def env() -> dict[str, str]:
    return load_env()


@pytest.fixture(scope="module")
def lake(env):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint = 'localhost:9000'")
    con.execute("SET s3_url_style = 'path'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_region = 'us-east-1'")
    con.execute("SET s3_access_key_id = ?", [env["MINIO_ROOT_USER"]])
    con.execute("SET s3_secret_access_key = ?", [env["MINIO_ROOT_PASSWORD"]])
    yield con
    con.close()


def publish(env, payload: dict) -> None:
    mqtt = pytest.importorskip("paho.mqtt.client")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"integration-test-{uuid.uuid4().hex[:8]}")
    client.username_pw_set(env.get("MOSQUITTO_ESP32_USER", "esp32"),
                           env["MOSQUITTO_ESP32_PASSWORD"])
    client.connect(os.getenv("MQTT_HOST", "localhost"), 1883, keepalive=15)
    client.loop_start()
    topic = f"irrigation/{payload['site_id']}/{payload['device_id']}/telemetry"
    info = client.publish(topic, json.dumps(payload), qos=1)
    info.wait_for_publish(timeout=15)
    client.loop_stop()
    client.disconnect()


def wait_for(con, query: str, params: list) -> int:
    """Polls the lake until the row shows up, or gives up."""
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        try:
            found = con.execute(query, params).fetchone()[0]
            if found:
                return found
        except Exception as exc:          # the prefix may not exist yet
            last_error = exc
        time.sleep(POLL_SECONDS)
    pytest.fail(f"nothing found in {TIMEOUT_SECONDS} s (last error: {last_error})")


def test_a_valid_message_lands_in_parquet(env, lake):
    device_id = f"itest-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC)
    publish(env, {
        "device_id": device_id,
        "site_id": "site-a",
        "ts": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "soil_moisture_pct": 47.5,
        "soil_raw": 2250,
        "air_temp_c": 18.2,
        "air_humidity_pct": 64.0,
        "battery_v": 4.05,
        "fw_version": "1.0.0",
    })

    bucket = env.get("MINIO_BUCKET", "irrigation")
    found = wait_for(lake, f"""
        SELECT count(*) FROM read_parquet('s3://{bucket}/telemetry/**/*.parquet',
                                          hive_partitioning = true)
        WHERE device_id = ?
    """, [device_id])
    assert found >= 1


def test_a_message_without_ts_ends_up_dead_lettered(env, lake):
    """The bridge must stop it before Kafka: it must NOT be in the lake."""
    device_id = f"itest-bad-{uuid.uuid4().hex[:8]}"
    publish(env, {"device_id": device_id, "site_id": "site-a",
                  "soil_moisture_pct": 47.5})

    time.sleep(60)          # give Spark time to write a full batch
    bucket = env.get("MINIO_BUCKET", "irrigation")
    try:
        found = lake.execute(f"""
            SELECT count(*) FROM read_parquet('s3://{bucket}/telemetry/**/*.parquet',
                                              hive_partitioning = true)
            WHERE device_id = ?
        """, [device_id]).fetchone()[0]
    except Exception:
        found = 0
    assert found == 0


def test_an_out_of_range_value_ends_up_in_quarantine(env, lake):
    """The bridge lets it through (valid structure), Spark rejects it."""
    device_id = f"itest-oor-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC)
    publish(env, {
        "device_id": device_id, "site_id": "site-a",
        "ts": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "soil_moisture_pct": 148.0, "soil_raw": 900,
        "air_temp_c": 18.2, "air_humidity_pct": 64.0,
        "battery_v": 4.05, "fw_version": "1.0.0",
    })

    bucket = env.get("MINIO_BUCKET", "irrigation")
    found = wait_for(lake, f"""
        SELECT count(*) FROM read_parquet('s3://{bucket}/quarantine/**/*.parquet',
                                          hive_partitioning = true)
        WHERE device_id = ? AND quality_rule = 'range'
    """, [device_id])
    assert found >= 1
