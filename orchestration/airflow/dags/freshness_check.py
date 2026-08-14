"""Device freshness: which sensors stopped reporting, and why.

Two independent sources are compared:

  * MongoDB -- when did each device last produce a *valid* reading?
  * MQTT    -- what does the broker's retained status topic say about it?

The cross-check is what makes the alert actionable:

  * silent AND the broker says "offline"  -> known outage (the device announced
    it, or its Last Will fired). Logged, not escalated.
  * silent AND the broker says "online"   -> the connection is alive but no data
    flows. Firmware hang, sensor failure, or a bridge that stopped forwarding.
    This is the case worth failing the run for.
  * silent AND no retained status at all  -> the device never connected, or the
    broker lost its state. Logged for investigation.

Note the imports inside the task functions: the DAG file is re-parsed by the
scheduler every few seconds, so top-level imports of heavy libraries would tax
every parse. Only what the file needs to *declare* the DAG belongs at the top.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

SILENCE_THRESHOLD_MINUTES = 15
MQTT_COLLECT_SECONDS = 5.0


@dag(
    dag_id="freshness_check",
    description="Flags silent devices, cross-checked with the retained MQTT status topic",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["monitoring"],
    doc_md=__doc__,
)
def freshness_check():

    @task
    def last_seen_from_mongo() -> list[dict]:
        """Age of the most recent valid reading, per device."""
        import os
        from datetime import datetime, timezone

        from pymongo import MongoClient

        client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=5000)
        try:
            collection = client[os.getenv("MONGO_DB", "irrigation")][
                os.getenv("MONGO_COLLECTION", "device_state")
            ]
            now = datetime.now(timezone.utc)
            rows = []
            for doc in collection.find({}, {"device_id": 1, "site_id": 1, "ts": 1}):
                ts = doc["ts"]
                if ts.tzinfo is None:           # Mongo renvoie du naif en UTC
                    ts = ts.replace(tzinfo=timezone.utc)
                rows.append({
                    "device_id": doc["device_id"],
                    "site_id": doc.get("site_id"),
                    "age_minutes": round((now - ts).total_seconds() / 60, 1),
                })
        finally:
            client.close()
        return sorted(rows, key=lambda r: r["device_id"])

    @task
    def status_from_mqtt() -> dict[str, str]:
        """Collects the retained status message of every device.

        Retained messages are delivered immediately on subscribe, so a short
        collection window is enough -- no need to wait for a publication.
        """
        import json
        import os
        import time

        import paho.mqtt.client as mqtt

        collected: dict[str, str] = {}

        def on_message(_client, _userdata, msg):
            try:
                payload = json.loads(msg.payload)
            except ValueError:
                return
            device_id = payload.get("device_id") or msg.topic.split("/")[-2]
            collected[device_id] = payload.get("status", "unknown")

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id="airflow-freshness-check")
        client.username_pw_set(os.getenv("MOSQUITTO_BRIDGE_USER", "bridge"),
                               os.environ["MOSQUITTO_BRIDGE_PASSWORD"])
        client.on_message = on_message
        client.connect(os.getenv("MQTT_HOST", "mosquitto"),
                       int(os.getenv("MQTT_PORT", "1883")), keepalive=15)
        client.subscribe("irrigation/+/+/status", qos=1)
        client.loop_start()
        time.sleep(MQTT_COLLECT_SECONDS)
        client.loop_stop()
        client.disconnect()
        return collected

    @task
    def reconcile(readings: list[dict], statuses: dict[str, str]) -> dict:
        """Classifies each silent device; fails only on the anomalous class."""
        import logging

        from airflow.exceptions import AirflowFailException

        log = logging.getLogger(__name__)
        healthy: list[str] = []
        expected_outage: list[str] = []
        ghost: list[str] = []
        unknown: list[str] = []

        for row in readings:
            device_id = row["device_id"]
            if row["age_minutes"] <= SILENCE_THRESHOLD_MINUTES:
                healthy.append(device_id)
                continue
            entry = f"{device_id} ({row['age_minutes']} min)"
            status = statuses.get(device_id)
            if status == "offline":
                expected_outage.append(entry)
            elif status == "online":
                ghost.append(entry)
            else:
                unknown.append(entry)

        log.info("healthy: %s", healthy or "none")
        if expected_outage:
            log.warning("silent, broker confirms offline: %s", expected_outage)
        if unknown:
            log.warning("silent, no retained status: %s", unknown)

        summary = {
            "healthy": len(healthy),
            "expected_outage": expected_outage,
            "ghost": ghost,
            "unknown": unknown,
        }

        if ghost:
            raise AirflowFailException(
                "Devices the broker believes are online but that produce no data: "
                + ", ".join(ghost)
            )
        return summary

    reconcile(last_seen_from_mongo(), status_from_mqtt())


freshness_check()
