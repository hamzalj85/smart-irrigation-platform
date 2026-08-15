#!/usr/bin/env python3
"""Pont MQTT -> Kafka.

Consomme la télémétrie sur Mosquitto et la republie dans Kafka, en gardant
la garantie de livraison : un message MQTT n'est acquitté qu'après
confirmation d'écriture par Kafka.

Validation *structurelle* uniquement (JSON lisible, champs obligatoires).
La validation *métier* (bornes, plausibilité) appartient à la couche Spark.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaException, Producer

LOG = logging.getLogger("bridge")

REQUIRED_FIELDS = ("device_id", "site_id", "ts")


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        LOG.error("Variable d'environnement manquante : %s", name)
        sys.exit(2)
    return value or ""


class Bridge:
    def __init__(self) -> None:
        self.mqtt_host = env("MQTT_HOST", "mosquitto")
        self.mqtt_port = int(env("MQTT_PORT", "1883"))
        self.mqtt_user = env("MOSQUITTO_BRIDGE_USER", "bridge")
        self.mqtt_password = env("MOSQUITTO_BRIDGE_PASSWORD", required=True)
        self.mqtt_topic = env("MQTT_TELEMETRY_TOPIC", "irrigation/+/+/telemetry")

        self.topic_ok = env("KAFKA_TELEMETRY_TOPIC", "irrigation.telemetry")
        self.topic_dlq = env("KAFKA_DLQ_TOPIC", "irrigation.telemetry.dlq")

        self.stats: Counter = Counter()
        self.running = True

        self.producer = Producer({
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            "client.id": "mqtt-kafka-bridge",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 50,
            "retries": 10,
            "delivery.timeout.ms": 120000,
        })

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="mqtt-kafka-bridge",
            clean_session=False,
            manual_ack=True,
        )
        self.client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    # -- Callbacks MQTT ---------------------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            LOG.error("Connexion MQTT refusée : %s", reason_code)
            return
        client.subscribe(self.mqtt_topic, qos=1)
        LOG.info("Connecté à MQTT, abonné à %s", self.mqtt_topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self.running:
            LOG.warning("Déconnecté de MQTT (%s), reconnexion auto", reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        self.stats["received"] += 1
        raw = msg.payload
        reason = None
        record = None

        try:
            record = json.loads(raw)
            if not isinstance(record, dict):
                reason = "not_an_object"
            else:
                missing = [f for f in REQUIRED_FIELDS if record.get(f) in (None, "")]
                if missing:
                    reason = f"missing_fields:{','.join(missing)}"
                else:
                    try:
                        datetime.fromisoformat(str(record["ts"]).replace("Z", "+00:00"))
                    except ValueError:
                        reason = "unparseable_ts"
        except json.JSONDecodeError as exc:
            reason = f"invalid_json:{exc.msg}"

        if reason is None:
            key = str(record["device_id"]).encode()
            self._produce(self.topic_ok, key, raw, msg)
            self.stats["forwarded"] += 1
        else:
            envelope = json.dumps({
                "reason": reason,
                "mqtt_topic": msg.topic,
                "received_at": datetime.now().astimezone().isoformat(),
                "payload": raw.decode("utf-8", errors="replace"),
            }).encode()
            key = (record or {}).get("device_id") if isinstance(record, dict) else None
            self._produce(self.topic_dlq, str(key).encode() if key else None, envelope, msg)
            self.stats["dead_lettered"] += 1
            self.stats[f"dlq:{reason.split(':')[0]}"] += 1
            LOG.warning("DLQ (%s) depuis %s", reason, msg.topic)

    # -- Kafka ------------------------------------------------------------
    def _produce(self, topic: str, key: bytes | None, value: bytes,
                 mqtt_msg: mqtt.MQTTMessage) -> None:
        def on_delivery(err, _kafka_msg, _mid=mqtt_msg.mid, _qos=mqtt_msg.qos):
            if err is not None:
                self.stats["delivery_failed"] += 1
                LOG.error("Échec d'écriture Kafka : %s (pas d'ACK MQTT)", err)
                return
            self.client.ack(_mid, _qos)

        while True:
            try:
                self.producer.produce(topic, key=key, value=value,
                                      on_delivery=on_delivery)
                return
            except BufferError:
                self.stats["backpressure"] += 1
                self.producer.poll(0.5)
            except KafkaException as exc:
                LOG.error("Erreur producteur Kafka : %s", exc)
                return

    def _poll_loop(self) -> None:
        while self.running:
            self.producer.poll(0.2)

    # -- Cycle de vie -----------------------------------------------------
    def run(self) -> int:
        threading.Thread(target=self._poll_loop, daemon=True).start()
        self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
        self.client.loop_start()

        last_report = time.monotonic()
        try:
            while self.running:
                time.sleep(1.0)
                if time.monotonic() - last_report >= 30:
                    LOG.info("reçus=%d transférés=%d dlq=%d en_attente=%d",
                             self.stats["received"], self.stats["forwarded"],
                             self.stats["dead_lettered"], len(self.producer))
                    last_report = time.monotonic()
        finally:
            LOG.info("Arrêt : vidage du tampon Kafka...")
            self.client.loop_stop()
            self.producer.flush(30)
            self.client.disconnect()
            for k, v in sorted(self.stats.items()):
                LOG.info("%-22s %d", k, v)
        return 0

    def stop(self, *_) -> None:
        self.running = False


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)-7s %(message)s")
    bridge = Bridge()
    signal.signal(signal.SIGINT, bridge.stop)
    signal.signal(signal.SIGTERM, bridge.stop)
    return bridge.run()


if __name__ == "__main__":
    sys.exit(main())
