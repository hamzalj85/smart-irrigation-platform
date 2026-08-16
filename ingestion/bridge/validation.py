"""Validation structurelle de la telemetrie, sans aucune dependance externe.

Ce module est volontairement isole du reste du pont : il n'importe ni paho,
ni confluent_kafka, ni prometheus_client. Uniquement la bibliotheque standard.

Pourquoi c'est important : la logique metier n'a aucune raison de trainer
derriere elle un client Kafka. La separer permet de la tester en trois
millisecondes, sans installer de dependance compilee, et rend evident ce qui
releve de la *decision* et ce qui releve du *transport*.

La frontiere de responsabilite est ici : ce module juge la **structure**
(JSON lisible, champs obligatoires, horodatage analysable). Le **sens** des
valeurs -- bornes physiques, coherence temporelle -- appartient aux portes
qualite de la couche Spark.
"""
from __future__ import annotations

import json
from datetime import datetime

REQUIRED_FIELDS = ("device_id", "site_id", "ts")


def validate_payload(raw: bytes | str) -> tuple[dict | None, str | None]:
    """Rend `(record, motif)`.

    `motif` vaut None si le message est structurellement valide. Sinon c'est
    une chaine `famille:detail` -- la famille permet d'agreger les rejets en
    supervision sans avoir a analyser le detail.

    La fonction ne leve jamais : un message invalide n'est pas une erreur de
    programme, c'est une donnee attendue dans un systeme reel.
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
