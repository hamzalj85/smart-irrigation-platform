"""Soil physics and sensor calibration.

Calibration is the kind of code you write once and never read again -- and
where a sign error goes unnoticed for months, because the values stay
plausible. Hence these tests.
"""
from __future__ import annotations

import argparse
import random
from collections import Counter

import pytest
from simulator import (
    ADC_DRY,
    ADC_WET,
    Device,
    inject_fault,
    moisture_to_raw,
    parse_duration,
    raw_to_moisture,
)


# --------------------------------------------------------------------------
# Resistive probe calibration
# --------------------------------------------------------------------------
@pytest.mark.parametrize("pct", [0.0, 12.5, 33.3, 50.0, 78.9, 100.0])
def test_calibration_round_trips(pct):
    """The two conversions must be reciprocal."""
    assert raw_to_moisture(moisture_to_raw(pct)) == pytest.approx(pct, abs=0.1)


def test_dry_soil_gives_the_high_adc_value():
    assert moisture_to_raw(0.0) == ADC_DRY


def test_saturated_soil_gives_the_low_adc_value():
    assert moisture_to_raw(100.0) == ADC_WET


def test_the_relationship_is_decreasing():
    """The wetter the soil, the less it resists: the curve goes down.

    A sign inversion here would produce perfectly plausible values and a model
    that learns the inverse of reality.
    """
    values = [moisture_to_raw(p) for p in range(0, 101, 10)]
    assert values == sorted(values, reverse=True)


def test_the_raw_value_stays_within_the_12_bit_adc_range():
    for pct in range(0, 101):
        assert 0 <= moisture_to_raw(pct) <= 4095


# --------------------------------------------------------------------------
# Duration parsing
# --------------------------------------------------------------------------
@pytest.mark.parametrize(("text", "seconds"), [
    ("5", 5), ("5s", 5), ("2m", 120), ("1h", 3600), ("0.5m", 30), (" 10s ", 10),
])
def test_parse_duration(text, seconds):
    assert parse_duration(text) == pytest.approx(seconds)


@pytest.mark.parametrize("text", ["", "abc", "5d", "-3s"])
def test_parse_duration_rejects_invalid_input(text):
    """An invalid duration must fail at argument parsing, not later."""
    with pytest.raises(argparse.ArgumentTypeError):
        parse_duration(text)


# --------------------------------------------------------------------------
# Soil physical model
# --------------------------------------------------------------------------
def test_the_soil_dries_when_it_is_not_irrigated():
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = 70.0
    device.irrigating = False
    device.step(600, rng)          # ten minutes
    assert device.moisture < 70.0


def test_irrigation_triggers_below_the_threshold():
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = device.irrigation_threshold + 0.05
    device.irrigating = False
    device.step(600, rng)
    assert device.irrigating


def test_irrigation_raises_moisture_then_stops():
    """The full cycle: this is the sawtooth visible in Grafana."""
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = 30.0
    device.irrigating = True
    for _ in range(200):
        device.step(60, rng)
        if not device.irrigating:
            break
    assert not device.irrigating, "irrigation never stopped"
    assert 80.0 <= device.moisture <= 95.0


def test_moisture_stays_within_physical_bounds():
    rng = random.Random(7)
    device = Device.create(1, "site-a", rng)
    for _ in range(5000):
        device.step(60, rng)
        assert 0.0 <= device.moisture <= 100.0


def test_each_device_has_its_own_drift():
    """Two cheap probes are never calibrated the same way."""
    rng = random.Random(42)
    devices = [Device.create(i, "site-a", rng) for i in range(1, 7)]
    biases = {round(d.calibration_bias, 6) for d in devices}
    assert len(biases) == 6


# --------------------------------------------------------------------------
# Fault injection
# --------------------------------------------------------------------------
BASE_PAYLOAD = {
    "device_id": "esp32-01",
    "ts": "2026-08-10T00:00:00.000Z",
    "soil_moisture_pct": 50.0,
    "air_temp_c": 20.0,
    "air_humidity_pct": 60.0,
}


def test_the_duplicate_fault_produces_two_identical_messages():
    rng = random.Random(0)
    stats: Counter = Counter()
    for _ in range(200):
        out = inject_fault(dict(BASE_PAYLOAD), rng, stats)
        if len(out) == 2:
            assert out[0] == out[1]
            return
    pytest.fail("no duplicate generated in 200 attempts")


def test_faults_are_accounted_for():
    rng = random.Random(0)
    stats: Counter = Counter()
    for _ in range(100):
        inject_fault(dict(BASE_PAYLOAD), rng, stats)
    assert sum(stats.values()) == 100
