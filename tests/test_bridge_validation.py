"""Structural validation of the MQTT -> Kafka bridge.

These tests need no broker, no Kafka, no Docker, not even the confluent-kafka
package: `validation.py` imports the standard library and nothing else.

That is exactly why the function was pulled out of the MQTT callback and then
out of `bridge.py` -- logic buried behind a Kafka client is only testable by
installing that client.
"""
from __future__ import annotations

import json

import pytest
from validation import validate_payload

VALID = {
    "device_id": "esp32-01",
    "site_id": "site-a",
    "ts": "2026-08-10T22:06:30.066Z",
    "soil_moisture_pct": 54.89,
    "soil_raw": 2102,
    "air_temp_c": 16.64,
    "air_humidity_pct": 68.76,
    "battery_v": 4.1,
    "fw_version": "1.0.0",
}


def payload(**overrides) -> bytes:
    record = {**VALID, **overrides}
    for key, value in overrides.items():
        if value is ...:
            record.pop(key)
    return json.dumps(record).encode()


def test_a_valid_message_passes():
    record, reason = validate_payload(payload())
    assert reason is None
    assert record["device_id"] == "esp32-01"


def test_unreadable_json_is_rejected():
    record, reason = validate_payload(b"this is not json at all")
    assert record is None
    assert reason.startswith("invalid_json:")


def test_valid_json_that_is_not_an_object():
    _, reason = validate_payload(b"[1, 2, 3]")
    assert reason == "not_an_object"


@pytest.mark.parametrize("field", ["device_id", "site_id", "ts"])
def test_a_mandatory_field_is_absent(field):
    _, reason = validate_payload(payload(**{field: ...}))
    assert reason == f"missing_fields:{field}"


@pytest.mark.parametrize("field", ["device_id", "site_id", "ts"])
def test_a_mandatory_field_is_empty_or_null(field):
    for value in (None, ""):
        _, reason = validate_payload(payload(**{field: value}))
        assert reason == f"missing_fields:{field}"


def test_every_missing_field_is_listed():
    _, reason = validate_payload(payload(device_id=..., site_id=...))
    assert reason == "missing_fields:device_id,site_id"


def test_an_unparseable_timestamp_is_caught():
    _, reason = validate_payload(payload(ts="yesterday evening"))
    assert reason == "unparseable_ts"


def test_the_record_is_returned_even_when_rejected():
    """The DLQ must still be keyable on device_id, even for a rejection."""
    record, reason = validate_payload(payload(ts=...))
    assert reason is not None
    assert record["device_id"] == "esp32-01"


def test_an_out_of_range_value_is_NOT_the_bridge_business():
    """Responsibility boundary: the bridge judges structure, not meaning.

    A moisture reading of 148 % is structurally valid. Quarantining it is
    Spark's job. This test documents the decision as much as it verifies it --
    if someone adds a bounds check here, it breaks.
    """
    _, reason = validate_payload(payload(soil_moisture_pct=148.0))
    assert reason is None


def test_the_reason_is_prefixed_by_a_family():
    """Monitoring aggregates on the part before the colon."""
    for bad, family in [(b"{", "invalid_json"),
                        (payload(ts=...), "missing_fields"),
                        (payload(ts="anything at all"), "unparseable_ts")]:
        _, reason = validate_payload(bad)
        assert reason.split(":")[0] == family
