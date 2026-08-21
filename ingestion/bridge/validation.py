"""Structural validation of the telemetry, with no external dependency.

This module is deliberately isolated from the rest of the bridge: it imports
neither paho, nor confluent_kafka, nor prometheus_client. Standard library only.

Why that matters: business logic has no reason to drag a Kafka client behind
it. Separating the two makes the logic testable in three milliseconds, with no
compiled dependency to install, and makes obvious what is a *decision* and what
is *transport*.

The responsibility boundary sits here: this module judges **structure**
(readable JSON, mandatory fields, parseable timestamp). The **meaning** of the
values -- physical bounds, temporal coherence -- belongs to the quality gates of
the Spark layer.
"""
from __future__ import annotations

import json
from datetime import datetime

REQUIRED_FIELDS = ("device_id", "site_id", "ts")


def validate_payload(raw: bytes | str) -> tuple[dict | None, str | None]:
    """Returns `(record, reason)`.

    `reason` is None when the message is structurally valid. Otherwise it is a
    `family:detail` string -- the family lets a dashboard aggregate rejections
    without having to parse the detail.

    The function never raises: an invalid message is not a programming error,
    it is expected data in a real system.
    """
    try:
        record = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid_json:{exc.msg}"

    if not isinstance(record, dict):
        return None, "not_an_object"

    missing = [f for f in REQUIRED_FIELDS if record.get(f) in (None, "")]
    if missing:
        return record, f"missing_fields:{','.join(missing)}"

    try:
        datetime.fromisoformat(str(record["ts"]).replace("Z", "+00:00"))
    except ValueError:
        return record, "unparseable_ts"

    return record, None
