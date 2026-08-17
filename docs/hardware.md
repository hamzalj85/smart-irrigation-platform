# Hardware: sensor node, wiring and calibration

The original thesis deployed physical sensor nodes in the field. This document
records the setup so the data contract and the quality thresholds used elsewhere
in the platform can be traced back to real hardware.

> **The firmware sketch is not published in this repository.** What follows is
> enough to rebuild a node and to understand where the numbers in
> `streaming/spark/telemetry_job.py` come from. The pipeline itself runs from
> `ingestion/simulator/simulator.py`, which reproduces the same physics and emits
> the same JSON on the same topics.

---

## Bill of materials

| Component | Model | Role |
| --- | --- | --- |
| Microcontroller | ESP32 DevKit v1 | acquisition, WiFi, MQTT client |
| Soil moisture | resistive probe, analog output | volumetric moisture proxy |
| Air temperature / humidity | DHT22 (AM2302) | ambient conditions |
| Power | 18650 LiPo cell + TP4056 charger | field autonomy |
| Enclosure | IP65 box, cable glands | outdoor protection |

A **capacitive** probe would be the better choice for a long deployment: the
resistive type corrodes within weeks because current flows continuously through
the soil. The resistive probe was used here for cost, and its drift is one of
the reasons the calibration bias is modelled per device in the simulator.

---

## Wiring

| Sensor pin | ESP32 pin | Note |
| --- | --- | --- |
| Soil probe AOUT | **GPIO 34** (ADC1_CH6) | input-only pin, ADC1 |
| Soil probe VCC | 3V3 | ideally switched by a GPIO, see below |
| Soil probe GND | GND | |
| DHT22 DATA | GPIO 4 | 10 kΩ pull-up to 3V3 |
| DHT22 VCC / GND | 3V3 / GND | |

### Why GPIO 34 and not any analog pin

The ESP32 has two ADCs. **ADC2 is unusable while WiFi is active** — the radio
driver owns it, and any read returns an error or garbage. Every analog sensor on
a WiFi-connected ESP32 must therefore be wired to an **ADC1** pin: GPIO 32–39.

This is the single most common wiring mistake on this board, and it produces a
symptom that looks like a data problem: readings that are plausible while
provisioning and nonsense once the network comes up.

### Powering the probe from a GPIO

Leaving the resistive probe permanently powered accelerates electrolysis. The
better arrangement is to drive its VCC from a GPIO, raise it, wait ~10 ms for
the reading to settle, sample, then drop it again. It also saves current between
measurements, which matters on a battery.

---

## Calibration

The probe returns an ADC value, not a percentage. The conversion is a straight
line between two measured extremes:

```
soil_raw = ADC_DRY - (moisture_pct / 100) × (ADC_DRY - ADC_WET)
```

with, on this hardware:

| Constant | Value | How it was obtained |
| --- | --- | --- |
| `ADC_DRY` | 3200 | probe in open air, completely dry |
| `ADC_WET` | 1200 | probe in a glass of water, fully submerged |

The relationship is **decreasing**: wet soil conducts better, so the measured
voltage — and the ADC value — drop. Getting this sign wrong produces perfectly
plausible values and a model that learns the inverse of reality, which is why
`tests/test_simulator.py` asserts the monotonic direction explicitly.

### Calibration procedure

1. Let the probe dry completely in open air, read the raw value 20 times over a
   minute, take the median → `ADC_DRY`.
2. Submerge the probe up to the marked line in water, same procedure →
   `ADC_WET`.
3. Write both constants into the firmware.
4. Repeat **per probe**. Two units from the same batch differ by several
   percent; the simulator models this as `calibration_bias`, a per-device
   Gaussian offset.

Recalibrate after a few weeks in soil: the electrodes corrode and both endpoints
drift. This is why `soil_raw` is kept in the payload alongside the percentage —
the raw value lets you recompute historical percentages with a corrected
calibration, which is impossible if only the derived value was stored.

---

## Where the quality thresholds come from

The bounds enforced in `streaming/spark/telemetry_job.py` are hardware limits,
not round numbers:

| Field | Bounds | Justification |
| --- | --- | --- |
| `soil_raw` | 0 – 4095 | ESP32 ADC is 12-bit |
| `soil_moisture_pct` | 0 – 100 | volumetric percentage |
| `air_temp_c` | -40 – 80 | DHT22 datasheet range, widened |
| `air_humidity_pct` | 0 – 100 | relative humidity |
| `battery_v` | 2.5 – 5.0 | LiPo cut-off to charger output |

A value outside these ranges cannot come from a working sensor: it means a
disconnected wire, a floating ground, or a failed read.

---

## Firmware behaviour

The sketch is not published, but the pipeline depends on four behaviours which
the simulator reproduces exactly:

1. **MQTT QoS 1** with a persistent session. `PubSubClient` is *not* suitable —
   it only supports QoS 0. The `arduino-mqtt` library (256dpi) does support QoS 1.
2. **A Last Will** registered at connect, publishing `status: offline` retained
   on `irrigation/<site>/<device>/status`. This is what allows the
   `freshness_check` DAG to distinguish a known outage from a silent failure.
3. **A retained `status: online`** published right after connecting.
4. **An ISO-8601 UTC timestamp** produced by the device, synchronised over NTP
   at boot. A device that restarts without NTP emits stale timestamps — which is
   exactly the `plausibility:stale_ts` case the quality gate catches, and which
   the simulator injects on purpose.

Credentials live in `firmware/esp32/secrets.h`, which is git-ignored. The
template `secrets.h.example` documents the required fields.

---

## Flashing

Either the Arduino IDE or PlatformIO from VS Code.

```
Board:        ESP32 Dev Module
Upload speed: 921600
Flash freq:   80 MHz
Partition:    Default 4MB with spiffs
```

Copy `secrets.h.example` to `secrets.h`, fill in the WiFi SSID, the WiFi
password, the broker address and the `esp32` MQTT credentials from your `.env`,
then upload. The device should appear within seconds on
`irrigation/+/+/status` — check with:

```bash
docker compose exec mosquitto mosquitto_sub -u bridge -P "$PW" -t 'irrigation/#' -v
```
