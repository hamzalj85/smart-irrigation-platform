# ESP32 firmware

**The sketch itself is not published in this repository.** This directory only
holds `secrets.h.example`, the template of the credentials file the firmware
expects (and which is git-ignored).

Everything needed to rebuild a node — bill of materials, wiring, the ADC1
constraint, the calibration procedure and the four firmware behaviours the
pipeline depends on — is documented in [`docs/hardware.md`](../../docs/hardware.md).

The pipeline runs from `ingestion/simulator/simulator.py`, which reproduces the
same soil physics and emits the same JSON on the same MQTT topics.
