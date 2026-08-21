#!/usr/bin/env python3
"""MQTT -> Kafka bridge.

Consumes telemetry from Mosquitto and republishes it into Kafka while keeping
the delivery guarantee: an MQTT message is only acknowledged once Kafka has
confirmed the write.

*Structural* validation only (readable JSON, mandatory fields). *Business*
validation (bounds, plausibility) belongs to the Spark layer.
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
from prometheus_client import Counter as PromCounter
from prometheus_client import Gauge, start_http_server
from validation import validate_payload

LOG = logging.getLogger("bridge")

# ---------------------------------------------------------------------------
# Prometheus metrics
#
# Convention: counters (_total) only ever grow, gauges give an instantaneous
# value. Prometheus computes the rates itself with rate() -- which is why we
# never expose a pre-computed "messages per second": that would be an average
# frozen over a window we chose on the operator's behalf.
# ---------------------------------------------------------------------------
MESSAGES = PromCounter(
    "irrigation_bridge_messages_total",
    "MQTT messages processed by the bridge",
    ["result"],                      # forwarded | dead_lettered
)
DEAD_LETTERS = PromCounter(
    "irrigation_bridge_dead_letters_total",
    "Messages rejected to the dead-letter queue, by reason family",
    ["reason"],
)
DELIVERY_FAILURES = PromCounter(
    "irrigation_bridge_delivery_failures_total",
    "Kafka write failures (the MQTT message is then left unacknowledged)",
)
BACKPRESSURE = PromCounter(
    "irrigation_bridge_backpressure_total",
    "Number of times the Kafka producer buffer was full",
)
QUEUE_DEPTH = Gauge(
    "irrigation_bridge_producer_queue_depth",
    "Messages waiting to be written in the producer buffer",
)
MQTT_CONNECTED = Gauge(
    "irrigation_bridge_mqtt_connected",
    "1 when the bridge is connected to the MQTT broker, 0 otherwise",
)


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        LOG.error("Missing environment variable: %s", name)
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

    # -- MQTT callbacks ---------------------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            LOG.error("MQTT connection refused: %s", reason_code)
            return
        client.subscribe(self.mqtt_topic, qos=1)
        MQTT_CONNECTED.set(1)
        LOG.info("Connected to MQTT, subscribed to %s", self.mqtt_topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        MQTT_CONNECTED.set(0)
        if self.running:
            LOG.warning("Disconnected from MQTT (%s), auto-reconnecting", reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        self.stats["received"] += 1
        raw = msg.payload
        record, reason = validate_payload(raw)

        if reason is None:
            key = str(record["device_id"]).encode()
            self._produce(self.topic_ok, key, raw, msg)
            self.stats["forwarded"] += 1
            MESSAGES.labels(result="forwarded").inc()
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
            MESSAGES.labels(result="dead_lettered").inc()
            DEAD_LETTERS.labels(reason=reason.split(":")[0]).inc()
            LOG.warning("DLQ (%s) from %s", reason, msg.topic)

    # -- Kafka ------------------------------------------------------------
    def _produce(self, topic: str, key: bytes | None, value: bytes,
                 mqtt_msg: mqtt.MQTTMessage) -> None:
        def on_delivery(err, _kafka_msg, _mid=mqtt_msg.mid, _qos=mqtt_msg.qos):
            if err is not None:
                self.stats["delivery_failed"] += 1
                DELIVERY_FAILURES.inc()
                LOG.error("Kafka write failed: %s (no MQTT ACK sent)", err)
                return
            self.client.ack(_mid, _qos)

        while True:
            try:
                self.producer.produce(topic, key=key, value=value,
                                      on_delivery=on_delivery)
                return
            except BufferError:
                self.stats["backpressure"] += 1
                BACKPRESSURE.inc()
                self.producer.poll(0.5)
            except KafkaException as exc:
                LOG.error("Kafka producer error: %s", exc)
                return

    def _poll_loop(self) -> None:
        while self.running:
            self.producer.poll(0.2)
            QUEUE_DEPTH.set(len(self.producer))

    # -- Lifecycle --------------------------------------------------------
    def run(self) -> int:
        # Dedicated metrics HTTP server, in its own thread: it must answer
        # even when the main loop is blocked.
        start_http_server(int(os.getenv("METRICS_PORT", "8000")))
        LOG.info("Prometheus metrics on :%s/metrics",
                 os.getenv("METRICS_PORT", "8000"))
        threading.Thread(target=self._poll_loop, daemon=True).start()
        self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
        self.client.loop_start()

        last_report = time.monotonic()
        try:
            while self.running:
                time.sleep(1.0)
                if time.monotonic() - last_report >= 30:
                    LOG.info("received=%d forwarded=%d dlq=%d pending=%d",
                             self.stats["received"], self.stats["forwarded"],
                             self.stats["dead_lettered"], len(self.producer))
                    last_report = time.monotonic()
        finally:
            LOG.info("Shutting down: flushing the Kafka buffer...")
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
