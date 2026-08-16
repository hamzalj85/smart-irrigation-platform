"""Validation structurelle du pont MQTT -> Kafka.

Ces tests ne demandent ni broker, ni Kafka, ni Docker, ni meme le paquet
confluent-kafka : `validation.py` n'importe que la bibliotheque standard.

C'est exactement pour cela que la fonction a ete sortie du callback MQTT puis
de `bridge.py` -- une logique enfouie derriere un client Kafka n'est testable
qu'en installant ce client.
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


def test_message_valide_passe():
    record, reason = validate_payload(payload())
    assert reason is None
    assert record["device_id"] == "esp32-01"


def test_json_illisible_est_rejete():
    record, reason = validate_payload(b"ceci n'est pas du json")
    assert record is None
    assert reason.startswith("invalid_json:")


def test_json_valide_mais_pas_un_objet():
    _, reason = validate_payload(b"[1, 2, 3]")
    assert reason == "not_an_object"


@pytest.mark.parametrize("champ", ["device_id", "site_id", "ts"])
def test_champ_obligatoire_absent(champ):
    _, reason = validate_payload(payload(**{champ: ...}))
    assert reason == f"missing_fields:{champ}"


@pytest.mark.parametrize("champ", ["device_id", "site_id", "ts"])
def test_champ_obligatoire_vide_ou_nul(champ):
    for valeur in (None, ""):
        _, reason = validate_payload(payload(**{champ: valeur}))
        assert reason == f"missing_fields:{champ}"


def test_plusieurs_champs_manquants_sont_tous_listes():
    _, reason = validate_payload(payload(device_id=..., site_id=...))
    assert reason == "missing_fields:device_id,site_id"


def test_horodatage_illisible():
    _, reason = validate_payload(payload(ts="hier soir"))
    assert reason == "unparseable_ts"


def test_le_record_est_rendu_meme_en_cas_de_rejet():
    """La DLQ doit pouvoir etre clee sur device_id, meme pour un rejet."""
    record, reason = validate_payload(payload(ts=...))
    assert reason is not None
    assert record["device_id"] == "esp32-01"


def test_une_valeur_hors_bornes_n_est_PAS_du_ressort_du_pont():
    """Frontiere de responsabilite : le pont juge la structure, pas le sens.

    Une humidite de 148 % est structurellement valide. C'est a Spark de la
    mettre en quarantaine. Ce test documente la decision autant qu'il la
    verifie -- si quelqu'un ajoute un controle de bornes ici, il casse.
    """
    _, reason = validate_payload(payload(soil_moisture_pct=148.0))
    assert reason is None


def test_le_motif_est_prefixe_par_une_famille():
    """La supervision agrege sur la partie avant les deux-points."""
    for mauvais, famille in [(b"{", "invalid_json"),
                             (payload(ts=...), "missing_fields"),
                             (payload(ts="n'importe quoi"), "unparseable_ts")]:
        _, reason = validate_payload(mauvais)
        assert reason.split(":")[0] == famille
