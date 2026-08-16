"""Physique du sol et calibration du capteur.

La calibration est le genre de code qu'on ecrit une fois et qu'on ne relit
jamais -- et dont une erreur de signe passe inapercue pendant des mois, parce
que les valeurs restent plausibles. D'ou ces tests.
"""
from __future__ import annotations

import argparse
import random

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
# Calibration du capteur resistif
# --------------------------------------------------------------------------
@pytest.mark.parametrize("pct", [0.0, 12.5, 33.3, 50.0, 78.9, 100.0])
def test_calibration_aller_retour(pct):
    """Les deux conversions doivent etre reciproques."""
    assert raw_to_moisture(moisture_to_raw(pct)) == pytest.approx(pct, abs=0.1)


def test_sol_sec_donne_la_valeur_haute_de_l_adc():
    assert moisture_to_raw(0.0) == ADC_DRY


def test_sol_sature_donne_la_valeur_basse_de_l_adc():
    assert moisture_to_raw(100.0) == ADC_WET


def test_la_relation_est_decroissante():
    """Plus le sol est humide, moins il resiste : la courbe descend.

    Une inversion de signe ici produirait des valeurs parfaitement
    plausibles et un modele qui apprend l'inverse de la realite.
    """
    valeurs = [moisture_to_raw(p) for p in range(0, 101, 10)]
    assert valeurs == sorted(valeurs, reverse=True)


def test_la_valeur_brute_reste_dans_la_plage_de_l_adc_12_bits():
    for pct in range(0, 101):
        assert 0 <= moisture_to_raw(pct) <= 4095


# --------------------------------------------------------------------------
# Analyse des durees
# --------------------------------------------------------------------------
@pytest.mark.parametrize(("texte", "secondes"), [
    ("5", 5), ("5s", 5), ("2m", 120), ("1h", 3600), ("0.5m", 30), (" 10s ", 10),
])
def test_parse_duration(texte, secondes):
    assert parse_duration(texte) == pytest.approx(secondes)


@pytest.mark.parametrize("texte", ["", "abc", "5j", "-3s"])
def test_parse_duration_refuse_l_invalide(texte):
    """Une duree invalide doit echouer a l'analyse des arguments, pas plus tard."""
    with pytest.raises(argparse.ArgumentTypeError):
        parse_duration(texte)


# --------------------------------------------------------------------------
# Modele physique du sol
# --------------------------------------------------------------------------
def test_le_sol_seche_quand_on_n_arrose_pas():
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = 70.0
    device.irrigating = False
    device.step(600, rng)          # dix minutes
    assert device.moisture < 70.0


def test_l_irrigation_se_declenche_sous_le_seuil():
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = device.irrigation_threshold + 0.05
    device.irrigating = False
    device.step(600, rng)
    assert device.irrigating


def test_l_irrigation_remonte_l_humidite_puis_s_arrete():
    """Le cycle complet : c'est la dent de scie visible dans Grafana."""
    rng = random.Random(1)
    device = Device.create(1, "site-a", rng)
    device.moisture = 30.0
    device.irrigating = True
    for _ in range(200):
        device.step(60, rng)
        if not device.irrigating:
            break
    assert not device.irrigating, "l'irrigation ne s'est jamais arretee"
    assert 80.0 <= device.moisture <= 95.0


def test_l_humidite_reste_dans_des_bornes_physiques():
    rng = random.Random(7)
    device = Device.create(1, "site-a", rng)
    for _ in range(5000):
        device.step(60, rng)
        assert 0.0 <= device.moisture <= 100.0


def test_chaque_appareil_a_sa_propre_derive():
    """Deux capteurs bon marche ne sont jamais calibres pareil."""
    rng = random.Random(42)
    devices = [Device.create(i, "site-a", rng) for i in range(1, 7)]
    biais = {round(d.calibration_bias, 6) for d in devices}
    assert len(biais) == 6


# --------------------------------------------------------------------------
# Injection de defauts
# --------------------------------------------------------------------------
def test_le_doublon_produit_deux_messages_identiques():
    rng = random.Random(0)
    stats: dict = __import__("collections").Counter()
    base = {"device_id": "esp32-01", "ts": "2026-08-10T00:00:00.000Z",
            "soil_moisture_pct": 50.0, "air_temp_c": 20.0,
            "air_humidity_pct": 60.0}
    for _ in range(200):
        out = inject_fault(dict(base), rng, stats)
        if len(out) == 2:
            assert out[0] == out[1]
            return
    pytest.fail("aucun doublon genere en 200 essais")


def test_les_defauts_sont_comptabilises():
    rng = random.Random(0)
    stats: dict = __import__("collections").Counter()
    base = {"device_id": "esp32-01", "ts": "2026-08-10T00:00:00.000Z",
            "soil_moisture_pct": 50.0, "air_temp_c": 20.0,
            "air_humidity_pct": 60.0}
    for _ in range(100):
        inject_fault(dict(base), rng, stats)
    assert sum(stats.values()) == 100
