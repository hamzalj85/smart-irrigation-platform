#!/usr/bin/env python3
"""Simulateur de capteurs d'irrigation.

Publie sur MQTT le même JSON, sur les mêmes topics, que le firmware ESP32.
Injecte volontairement des défauts : c'est la matière première des portes
qualité de la couche streaming.
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

LOG = logging.getLogger("simulator")

# Bornes physiques du capteur résistif (ADC 12 bits de l'ESP32).
ADC_DRY = 3200   # sol totalement sec
ADC_WET = 1200   # sol saturé
FW_VERSION = "1.0.0"


# --------------------------------------------------------------------------
# Modèle physique
# --------------------------------------------------------------------------
@dataclass
class Device:
    """Un ESP32 simulé, avec son état et ses imperfections propres."""

    device_id: str
    site_id: str
    moisture: float                  # % volumétrique courant
    dry_rate: float                  # % perdus par minute
    irrigation_threshold: float      # seuil de déclenchement
    calibration_bias: float          # dérive permanente du capteur, en %
    battery: float = 4.10
    irrigating: bool = False
    offline_until: float = 0.0

    @classmethod
    def create(cls, index: int, site_id: str, rng: random.Random) -> "Device":
        return cls(
            device_id=f"esp32-{index:02d}",
            site_id=site_id,
            moisture=rng.uniform(35.0, 75.0),
            # Chaque appareil est dans un sol différent : sable ou argile
            # ne sèchent pas à la même vitesse.
            dry_rate=rng.uniform(0.04, 0.16),
            irrigation_threshold=rng.uniform(28.0, 35.0),
            # Aucun capteur bon marché n'est calibré comme son voisin.
            calibration_bias=rng.gauss(0.0, 1.8),
        )

    def step(self, elapsed_s: float, rng: random.Random) -> None:
        """Fait avancer l'état physique de `elapsed_s` secondes."""
        minutes = elapsed_s / 60.0

        if self.irrigating:
            # L'irrigation remonte vite l'humidité, puis s'arrête.
            self.moisture += 9.0 * minutes
            if self.moisture >= 85.0:
                self.moisture = min(self.moisture, 92.0)
                self.irrigating = False
        else:
            # Décroissance lente, plus rapide quand le sol est humide
            # (évaporation proportionnelle à la teneur en eau).
            self.moisture -= self.dry_rate * minutes * (0.5 + self.moisture / 100.0)
            if self.moisture <= self.irrigation_threshold:
                self.irrigating = True
                LOG.info("%s: irrigation déclenchée à %.1f%%",
                         self.device_id, self.moisture)

        self.moisture += rng.gauss(0.0, 0.15)          # bruit de mesure
        self.moisture = max(0.0, min(100.0, self.moisture))
        self.battery = max(3.30, self.battery - 0.00004 * minutes)

    def reading(self, now: datetime, rng: random.Random) -> dict:
        """Produit le JSON tel que le firmware l'émettrait."""
        measured = self.moisture + self.calibration_bias
        raw = int(ADC_DRY - (measured / 100.0) * (ADC_DRY - ADC_WET))

        # Cycle jour/nuit : pic de température vers 15h, minimum vers 3h.
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
# Injection de défauts
# --------------------------------------------------------------------------
FAULT_KINDS = ("null_value", "out_of_range", "duplicate", "stale_ts", "malformed")


def inject_fault(payload: dict, rng: random.Random, stats: Counter) -> list[dict]:
    """Corrompt une lecture. Retourne la liste des messages à publier."""
    kind = rng.choice(FAULT_KINDS)
    stats[kind] += 1
    bad = dict(payload)

    if kind == "null_value":
        # Lecture DHT22 ratée : le capteur renvoie NaN une fois sur cent.
        bad[rng.choice(["air_temp_c", "air_humidity_pct", "soil_moisture_pct"])] = None

    elif kind == "out_of_range":
        # Câble débranché ou masse flottante : valeur physiquement impossible.
        bad["soil_moisture_pct"] = rng.choice([-12.4, 148.0, 999.9])

    elif kind == "duplicate":
        # QoS 1 = at-least-once. Le même message arrive deux fois.
        return [bad, dict(bad)]

    elif kind == "stale_ts":
        # Horloge non synchronisée après un reset : l'appareil réémet
        # des données vieilles de plusieurs heures.
        old = datetime.now(timezone.utc) - timedelta(hours=rng.uniform(2, 30))
        bad["ts"] = old.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    elif kind == "malformed":
        # Buffer tronqué côté firmware : un champ obligatoire disparaît.
        bad.pop(rng.choice(["device_id", "ts", "soil_moisture_pct"]), None)

    return [bad]


# --------------------------------------------------------------------------
# Utilitaires
# --------------------------------------------------------------------------
def parse_duration(value: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([smh]?)", value.strip().lower())
    if not m:
        raise argparse.ArgumentTypeError(f"durée invalide: {value!r} (ex: 5s, 2m)")
    return float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]


def unit_ratio(value: str) -> float:
    f = float(value)
    if not 0.0 <= f <= 1.0:
        raise argparse.ArgumentTypeError("doit être entre 0.0 et 1.0")
    return f


def telemetry_topic(dev: Device) -> str:
    return f"irrigation/{dev.site_id}/{dev.device_id}/telemetry"


def status_topic(dev: Device) -> str:
    return f"irrigation/{dev.site_id}/{dev.device_id}/status"


def make_client(dev: Device, args) -> mqtt.Client:
    """Un client MQTT par appareil, comme dans la réalité."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=dev.device_id,
        clean_session=False,          # session persistante : QoS 1 survit aux coupures
    )
    client.username_pw_set(args.username, args.password)

    # Last Will : si l'appareil disparaît brutalement, le broker publie
    # ce message à sa place. C'est le broker qui détecte la panne.
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
# Programme principal
# --------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description="Simulateur de capteurs d'irrigation")
    p.add_argument("--sensors", type=int, default=3)
    p.add_argument("--interval", type=parse_duration, default="10s",
                   help="délai entre deux lectures (ex: 5s, 2m)")
    p.add_argument("--fault-rate", type=unit_ratio, default=0.0,
                   help="fraction de lectures corrompues")
    p.add_argument("--offline-rate", type=unit_ratio, default=0.0,
                   help="probabilité par cycle qu'un appareil se taise")
    p.add_argument("--speed", type=float, default=1.0,
                   help="accélère la physique du sol (120 = 1 cycle en ~2 min)")    
    p.add_argument("--site-id", default="site-a")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", 1883)))
    p.add_argument("--username", default=os.getenv("MOSQUITTO_ESP32_USER", "esp32"))
    p.add_argument("--password", default=os.getenv("MOSQUITTO_ESP32_PASSWORD"))
    p.add_argument("--seed", type=int, default=None,
                   help="graine aléatoire, pour des runs reproductibles")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    if not args.password:
        LOG.error("Mot de passe manquant : --password ou MOSQUITTO_ESP32_PASSWORD")
        return 2

    rng = random.Random(args.seed)
    devices = [Device.create(i + 1, args.site_id, rng) for i in range(args.sensors)]
    clients = {d.device_id: make_client(d, args) for d in devices}
    LOG.info("%d appareil(s) connecté(s) à %s:%d", len(devices), args.host, args.port)

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
            now = datetime.now(timezone.utc)

            for dev in devices:
                client = clients[dev.device_id]

                # L'appareil est-il dans une fenêtre de silence ?
                if dev.offline_until > cycle_start:
                    dev.step(args.interval * args.speed, rng)   # le sol continue de sécher
                    continue

                if rng.random() < args.offline_rate:
                    cycles = rng.randint(2, 6)
                    dev.offline_until = cycle_start + cycles * args.interval
                    stats["offline"] += 1
                    LOG.warning("%s: hors ligne pour %d cycles",
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

        LOG.info("--- Résumé ---")
        for key, value in sorted(stats.items()):
            LOG.info("%-14s %d", key, value)

    return 0


if __name__ == "__main__":
    sys.exit(main())