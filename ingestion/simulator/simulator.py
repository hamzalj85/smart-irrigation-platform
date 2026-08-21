#!/usr/bin/env python3
"""Irrigation sensor simulator.

Publishes the same JSON, on the same MQTT topics, as the ESP32 firmware would.
It injects faults on purpose: those are the raw material of the quality gates
in the streaming layer.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import paho.mqtt.client as mqtt

LOG = logging.getLogger("simulator")

# Physical bounds of the resistive probe (12-bit ESP32 ADC).
ADC_DRY = 3200   # completely dry soil
ADC_WET = 1200   # saturated soil
FW_VERSION = "1.0.0"


def moisture_to_raw(moisture_pct: float) -> int:
    """Converts a moisture percentage into a raw ADC value.

    This is the calibration of the resistive probe, and it is decreasing: the
    wetter the soil, the less it resists, the lower the measured voltage.
    Dry soil -> 3200, saturated soil -> 1200.
    """
    return int(ADC_DRY - (moisture_pct / 100.0) * (ADC_DRY - ADC_WET))


def raw_to_moisture(raw: int) -> float:
    """Inverse of `moisture_to_raw`: what the firmware itself would compute.

    The two functions must be exactly reciprocal, and a test asserts it. A
    calibration that does not round-trip is a silent bug you only notice by
    comparing against a reference measurement.
    """
    return (ADC_DRY - raw) / (ADC_DRY - ADC_WET) * 100.0


# --------------------------------------------------------------------------
# Physical model
# --------------------------------------------------------------------------
@dataclass
class Device:
    """A simulated ESP32, with its own state and its own imperfections."""

    device_id: str
    site_id: str
    moisture: float                  # current volumetric %
    dry_rate: float                  # % lost per minute
    irrigation_threshold: float      # trigger threshold
    calibration_bias: float          # permanent sensor drift, in %
    battery: float = 4.10
    irrigating: bool = False
    offline_until: float = 0.0

    @classmethod
    def create(cls, index: int, site_id: str, rng: random.Random) -> Device:
        return cls(
            device_id=f"esp32-{index:02d}",
            site_id=site_id,
            moisture=rng.uniform(35.0, 75.0),
            # Each device sits in a different soil: sand and clay do not dry
            # at the same speed.
            dry_rate=rng.uniform(0.04, 0.16),
            irrigation_threshold=rng.uniform(28.0, 35.0),
            # No cheap probe is calibrated like its neighbour.
            calibration_bias=rng.gauss(0.0, 1.8),
        )

    def step(self, elapsed_s: float, rng: random.Random) -> None:
        """Advances the physical state by `elapsed_s` seconds."""
        minutes = elapsed_s / 60.0

        if self.irrigating:
            # Irrigation lifts moisture quickly, then stops.
            self.moisture += 9.0 * minutes
            if self.moisture >= 85.0:
                self.moisture = min(self.moisture, 92.0)
                self.irrigating = False
        else:
            # Slow decay, faster when the soil is wet (evaporation is
            # proportional to the water content).
            self.moisture -= self.dry_rate * minutes * (0.5 + self.moisture / 100.0)
            if self.moisture <= self.irrigation_threshold:
                self.irrigating = True
                LOG.info("%s: irrigation triggered at %.1f%%",
                         self.device_id, self.moisture)

        self.moisture += rng.gauss(0.0, 0.15)          # measurement noise
        self.moisture = max(0.0, min(100.0, self.moisture))
        self.battery = max(3.30, self.battery - 0.00004 * minutes)

    def reading(self, now: datetime, rng: random.Random) -> dict:
        """Produces the JSON exactly as the firmware would emit it."""
        measured = self.moisture + self.calibration_bias
        raw = moisture_to_raw(measured)

        # Day/night cycle: temperature peaks around 15:00, bottoms near 03:00.
        hour = now.hour + now.minute / 60.0
        temp = 19.0 + 7.5 * math.sin(2 * math.pi * (hour - 9.0) / 24.0)
        temp += rng.gauss(0.0, 0.4)
        humidity = max(20.0, min(95.0, 95.0 - 1.6 * temp + rng.gauss(0.0, 2.0)))

        return {
            "device_id": self.device_id,
            "site_id": self.site_id,
            "ts": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "soil_moisture_pct": round(max(0.0, min(100.0, measured)), 2),
            "soil_raw": raw,
            "air_temp_c": round(temp, 2),
            "air_humidity_pct": round(humidity, 2),
            "battery_v": round(self.battery, 3),
            "fw_version": FW_VERSION,
        }


# --------------------------------------------------------------------------
# Fault injection
# --------------------------------------------------------------------------
FAULT_KINDS = ("null_value", "out_of_range", "duplicate", "stale_ts", "malformed")


def inject_fault(payload: dict, rng: random.Random, stats: Counter) -> list[dict]:
    """Corrupts a reading. Returns the list of messages to publish."""
    kind = rng.choice(FAULT_KINDS)
    stats[kind] += 1
    bad = dict(payload)

    if kind == "null_value":
        # Failed DHT22 read: the sensor returns NaN about once in a hundred.
        bad[rng.choice(["air_temp_c", "air_humidity_pct", "soil_moisture_pct"])] = None

    elif kind == "out_of_range":
        # Unplugged cable or floating ground: a physically impossible value.
        bad["soil_moisture_pct"] = rng.choice([-12.4, 148.0, 999.9])

    elif kind == "duplicate":
        # QoS 1 = at-least-once. The same message arrives twice.
        return [bad, dict(bad)]

    elif kind == "stale_ts":
        # Clock not synchronised after a reset: the device re-emits data that
        # is several hours old.
        old = datetime.now(UTC) - timedelta(hours=rng.uniform(2, 30))
        bad["ts"] = old.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    elif kind == "malformed":
        # Truncated buffer in the firmware: a mandatory field disappears.
        bad.pop(rng.choice(["device_id", "ts", "soil_moisture_pct"]), None)

    return [bad]


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def parse_duration(value: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([smh]?)", value.strip().lower())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid duration: {value!r} (e.g. 5s, 2m)")
    return float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]


def unit_ratio(value: str) -> float:
    f = float(value)
    if not 0.0 <= f <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0.0 and 1.0")
    return f


def telemetry_topic(dev: Device) -> str:
    return f"irrigation/{dev.site_id}/{dev.device_id}/telemetry"


def status_topic(dev: Device) -> str:
    return f"irrigation/{dev.site_id}/{dev.device_id}/status"


def make_client(dev: Device, args) -> mqtt.Client:
    """One MQTT client per device, exactly as in the field."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=dev.device_id,
        clean_session=False,          # persistent session: QoS 1 survives outages
    )
    client.username_pw_set(args.username, args.password)

    # Last Will: if the device disappears abruptly, the broker publishes this
    # message on its behalf. The broker is what detects the outage.
    client.will_set(
        status_topic(dev),
        json.dumps({"device_id": dev.device_id, "status": "offline",
                    "reason": "last_will"}),
        qos=1,
        retain=True,
    )
    client.connect(args.host, args.port, keepalive=30)
    client.loop_start()
    client.publish(
        status_topic(dev),
        json.dumps({"device_id": dev.device_id, "status": "online"}),
        qos=1,
        retain=True,
    )
    return client


# --------------------------------------------------------------------------
# Main program
# --------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description="Irrigation sensor simulator")
    p.add_argument("--sensors", type=int, default=3)
    p.add_argument("--interval", type=parse_duration, default="10s",
                   help="delay between two readings (e.g. 5s, 2m)")
    p.add_argument("--fault-rate", type=unit_ratio, default=0.0,
                   help="fraction of readings made faulty on purpose")
    p.add_argument("--offline-rate", type=unit_ratio, default=0.0,
                   help="per-cycle probability that a device goes silent")
    p.add_argument("--speed", type=float, default=1.0,
                   help="accelerates the soil physics (120 = one cycle in ~2 min)")
    p.add_argument("--site-id", default="site-a")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    p.add_argument("--username", default=os.getenv("MOSQUITTO_ESP32_USER", "esp32"))
    p.add_argument("--password", default=os.getenv("MOSQUITTO_ESP32_PASSWORD"))
    p.add_argument("--seed", type=int, default=None,
                   help="random seed, for reproducible runs")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    if not args.password:
        LOG.error("Missing password: use --password or MOSQUITTO_ESP32_PASSWORD")
        return 2

    rng = random.Random(args.seed)
    devices = [Device.create(i + 1, args.site_id, rng) for i in range(args.sensors)]
    clients = {d.device_id: make_client(d, args) for d in devices}
    LOG.info("%d device(s) connected to %s:%d", len(devices), args.host, args.port)

    stats: Counter = Counter()
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while running:
            cycle_start = time.monotonic()
            now = datetime.now(UTC)

            for dev in devices:
                client = clients[dev.device_id]

                # Is the device inside a silence window?
                if dev.offline_until > cycle_start:
                    dev.step(args.interval * args.speed, rng)   # the soil keeps drying
                    continue

                if rng.random() < args.offline_rate:
                    cycles = rng.randint(2, 6)
                    dev.offline_until = cycle_start + cycles * args.interval
                    stats["offline"] += 1
                    LOG.warning("%s: going offline for %d cycles",
                                dev.device_id, cycles)
                    client.publish(
                        status_topic(dev),
                        json.dumps({"device_id": dev.device_id,
                                    "status": "offline", "reason": "simulated"}),
                        qos=1, retain=True)
                    continue

                dev.step(args.interval * args.speed, rng)
                payload = dev.reading(now, rng)

                if rng.random() < args.fault_rate:
                    messages = inject_fault(payload, rng, stats)
                else:
                    stats["clean"] += 1
                    messages = [payload]

                for msg in messages:
                    client.publish(telemetry_topic(dev),
                                   json.dumps(msg), qos=1)
                stats["published"] += len(messages)

            elapsed = time.monotonic() - cycle_start
            time.sleep(max(0.0, args.interval - elapsed))
    finally:
        for dev in devices:
            clients[dev.device_id].publish(
                status_topic(dev),
                json.dumps({"device_id": dev.device_id, "status": "offline",
                            "reason": "shutdown"}),
                qos=1, retain=True)
            clients[dev.device_id].loop_stop()
            clients[dev.device_id].disconnect()

        LOG.info("--- Summary ---")
        for key, value in sorted(stats.items()):
            LOG.info("%-14s %d", key, value)

    return 0


if __name__ == "__main__":
    sys.exit(main())
