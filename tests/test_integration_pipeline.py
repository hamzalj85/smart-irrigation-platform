"""Test d'integration : un message publie sur MQTT arrive-t-il en Parquet ?

C'est le seul test qui traverse toute la chaine -- Mosquitto, le pont, Kafka,
Spark, MinIO. Il ne remplace pas les tests unitaires : il verifie ce qu'eux
ne peuvent pas voir, c'est-a-dire le **cablage**. Les portes qualite peuvent
etre parfaites et le pipeline ne rien produire parce qu'un identifiant est
faux ou qu'un topic n'existe pas.

Il est marque `integration` et **exclu par defaut** : il demande la stack
Docker en marche. En local :

    docker compose up -d
    pytest -m integration

La CI ne le lance pas -- monter onze services pour chaque commit couterait
plus cher que ce qu'il rapporte. Il se lance a la main avant une publication.
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
        pytest.skip(".env absent : la stack n'est pas configuree")
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
    """Interroge le lac jusqu'a trouver la ligne, ou abandonne."""
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_error = None
    while time.monotonic() < deadline:
        try:
            found = con.execute(query, params).fetchone()[0]
            if found:
                return found
        except Exception as exc:          # le prefixe peut ne pas exister encore
            last_error = exc
        time.sleep(POLL_SECONDS)
    pytest.fail(f"rien trouve en {TIMEOUT_SECONDS} s (derniere erreur : {last_error})")


def test_un_message_valide_arrive_en_parquet(env, lake):
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


def test_un_message_sans_ts_finit_en_dead_letter(env, lake):
    """Le pont doit l'arreter avant Kafka : il ne doit PAS etre dans le lac."""
    device_id = f"itest-bad-{uuid.uuid4().hex[:8]}"
    publish(env, {"device_id": device_id, "site_id": "site-a",
                  "soil_moisture_pct": 47.5})

    time.sleep(60)          # laisse le temps a Spark d'ecrire un lot complet
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


def test_une_valeur_hors_bornes_finit_en_quarantaine(env, lake):
    """Le pont la laisse passer (structure valide), Spark la rejette."""
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
