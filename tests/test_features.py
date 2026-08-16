"""Construction des variables et decoupage temporel du modele.

Le test central de ce fichier est `test_aucune_fuite_du_futur`. Une fuite de
donnees ne provoque aucune erreur : elle produit un score flatteur et un
modele inutilisable. C'est le bug le plus couteux de la discipline, et le
seul moyen de s'en premunir est de l'ecrire noir sur blanc dans un test.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from features import (
    FEATURE_COLUMNS,
    IRRIGATION_THRESHOLD_PCT,
    TARGET_COLUMN,
    build_features,
    rule_baseline,
    time_split,
)


def hourly_frame(devices=2, hours=48, start="2026-07-01") -> pd.DataFrame:
    """Une table horaire synthetique en dents de scie, comme fct_readings_hourly."""
    rows = []
    times = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    for d in range(1, devices + 1):
        moisture = 80.0
        for t in times:
            moisture -= 2.0
            if moisture < 30.0:
                moisture = 85.0
            rows.append({
                "device_id": f"esp32-{d:02d}",
                "site_id": "site-a",
                "hour_start": t,
                "reading_count": 12,
                "avg_soil_moisture_pct": moisture,
                "min_soil_moisture_pct": moisture - 1.0,
                "max_soil_moisture_pct": moisture + 1.0,
                "soil_moisture_range_pct": 2.0,
                "avg_air_temp_c": 20.0 + t.hour * 0.1,
                "avg_air_humidity_pct": 60.0,
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Fuite de donnees
# --------------------------------------------------------------------------
def test_aucune_fuite_du_futur():
    """Modifier le futur ne doit changer AUCUNE variable explicative.

    On construit deux jeux identiques sauf la derniere heure, puis on compare
    les variables des heures precedentes. Si une variable regarde en avant,
    elle differe et le test tombe.
    """
    base = hourly_frame(devices=1, hours=48)
    altere = base.copy()
    derniere = altere.index[-1]
    altere.loc[derniere, ["avg_soil_moisture_pct", "min_soil_moisture_pct"]] = [5.0, 4.0]

    f_base = build_features(base)
    f_altere = build_features(altere)

    commun = min(len(f_base), len(f_altere)) - 1      # on exclut la ligne modifiee
    pd.testing.assert_frame_equal(
        f_base.loc[: commun - 1, FEATURE_COLUMNS],
        f_altere.loc[: commun - 1, FEATURE_COLUMNS],
    )


def test_la_cible_regarde_bien_l_heure_suivante():
    frame = hourly_frame(devices=1, hours=48)
    features = build_features(frame, threshold=IRRIGATION_THRESHOLD_PCT)
    fusion = features.merge(
        frame[["device_id", "hour_start", "min_soil_moisture_pct"]]
        .assign(hour_start=lambda d: d["hour_start"] - pd.Timedelta(hours=1))
        .rename(columns={"min_soil_moisture_pct": "min_suivant"}),
        on=["device_id", "hour_start"], how="inner")
    attendu = (fusion["min_suivant"] < IRRIGATION_THRESHOLD_PCT).astype(float)
    assert (fusion[TARGET_COLUMN] == attendu).all()


def test_les_retards_sont_calcules_par_appareil():
    """Un decalage global melangerait les capteurs entre eux."""
    frame = hourly_frame(devices=3, hours=30)
    features = build_features(frame)
    for _, groupe in features.groupby("device_id"):
        ecart = groupe["hour_start"].diff().dropna()
        assert (ecart == pd.Timedelta(hours=1)).all()


def test_l_heure_est_encodee_sur_un_cercle():
    """23 h et 00 h doivent etre voisines, pas aux antipodes."""
    frame = hourly_frame(devices=1, hours=48)
    features = build_features(frame).set_index("hour_start")
    minuit = features[features.index.hour == 0].iloc[0]
    onze = features[features.index.hour == 23].iloc[0]
    distance = np.hypot(minuit["hour_sin"] - onze["hour_sin"],
                        minuit["hour_cos"] - onze["hour_cos"])
    assert distance < 0.3


def test_aucune_valeur_manquante_en_sortie():
    features = build_features(hourly_frame(devices=2, hours=48))
    assert not features[FEATURE_COLUMNS].isna().any().any()


# --------------------------------------------------------------------------
# Decoupage temporel
# --------------------------------------------------------------------------
def test_le_decoupage_est_chronologique():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert train["hour_start"].max() <= test["hour_start"].min()


def test_aucune_heure_ne_se_retrouve_des_deux_cotes():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert set(train["hour_start"]).isdisjoint(set(test["hour_start"]))


def test_les_proportions_sont_respectees():
    features = build_features(hourly_frame(devices=2, hours=200))
    train, test = time_split(features, 0.7)
    assert len(train) + len(test) == len(features)
    assert 0.6 < len(train) / len(features) < 0.8


def test_un_cote_vide_echoue_bruyamment():
    """Mieux vaut une erreur qu'un jeu de test vide et un score absurde.

    Cinq heures d'historique pour un seul appareil ne laissent qu'une ligne
    exploitable une fois les trois retards et la cible calcules : le cote test
    serait vide. `time_split` refuse plutot que de rendre un DataFrame vide,
    sur lequel les metriques vaudraient 0 sans que rien ne l'explique.

    Note : `time_split` ne juge que la validite structurelle du decoupage.
    Le volume suffisant pour entrainer est du ressort de `train.py`, qui
    refuse en dessous de `--min-rows`.
    """
    frame = build_features(hourly_frame(devices=1, hours=4))
    assert len(frame) == 1, "quatre heures ne laissent qu'une ligne exploitable"
    with pytest.raises(ValueError, match="Empty split"):
        time_split(frame, 0.7)


def test_deux_lignes_suffisent_structurellement():
    """La frontiere exacte, documentee : deux lignes se coupent en 1 + 1.

    Cinq heures d'historique donnent deux lignes : trois sont perdues par les
    retards a 1, 2 et 3 heures, une par la cible qui regarde l'heure suivante.
    """
    frame = build_features(hourly_frame(devices=1, hours=5))
    assert len(frame) == 2
    train, test = time_split(frame, 0.7)
    assert len(train) == 1
    assert len(test) == 1


@pytest.mark.parametrize("fraction", [0.0, 1.0, -0.5, 1.5])
def test_fraction_invalide_refusee(fraction):
    with pytest.raises(ValueError):
        time_split(build_features(hourly_frame(devices=2, hours=100)), fraction)


# --------------------------------------------------------------------------
# Reference metier
# --------------------------------------------------------------------------
def test_la_regle_metier_est_deterministe():
    features = build_features(hourly_frame(devices=2, hours=60))
    assert (rule_baseline(features) == rule_baseline(features)).all()


def test_la_regle_metier_ne_rend_que_des_0_et_des_1():
    features = build_features(hourly_frame(devices=2, hours=60))
    assert set(rule_baseline(features).unique()) <= {0, 1}
