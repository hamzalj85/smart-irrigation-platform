"""Les trois portes qualite de la couche streaming.

Ces tests demandent PySpark et une JVM, mais **pas** Kafka, ni MinIO, ni
MongoDB : `apply_quality_gates` prend un DataFrame et en rend un autre. Le
job de streaming n'est qu'un cablage autour de cette fonction pure.

Marque `spark` : `pytest -m "not spark"` les exclut sur une machine sans JVM.
"""
from __future__ import annotations

import datetime as dt

import pytest

pytest.importorskip("pyspark", reason="PySpark absent")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from telemetry_job import (
    MAX_LATE_HOURS,
    TELEMETRY_SCHEMA,
    apply_quality_gates,
    parse_telemetry,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[1]")
               .appName("tests-quality-gates")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def kafka_frame(spark, payloads, ingested_at=None):
    """Imite la forme d'un DataFrame lu depuis le connecteur Kafka."""
    ingested_at = ingested_at or dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
    rows = [(name, text) for name, text in payloads]
    return (spark.createDataFrame(rows, "cas string, value string")
            .withColumn("key", F.lit("k").cast("binary"))
            .withColumn("partition", F.lit(0))
            .withColumn("offset", F.lit(0).cast("long"))
            .withColumn("timestamp", F.lit(ingested_at).cast("timestamp"))
            .withColumn("value", F.col("value").cast("binary")))


def verdicts(spark, payloads, ingested_at=None) -> dict[str, str | None]:
    """Rend {libelle du cas: motif de rejet}, une passe par cas."""
    out: dict[str, str | None] = {}
    for name, text in payloads:
        frame = kafka_frame(spark, [(name, text)], ingested_at)
        row = apply_quality_gates(parse_telemetry(frame)).collect()[0]
        out[name] = row["quality_reason"]
    return out


def message(**overrides) -> str:
    import json
    base = {
        "device_id": "esp32-01", "site_id": "site-a",
        "ts": "2026-08-10T11:59:00.000Z",
        "soil_moisture_pct": 54.89, "soil_raw": 2102,
        "air_temp_c": 16.64, "air_humidity_pct": 68.76,
        "battery_v": 4.1, "fw_version": "1.0.0",
    }
    base.update(overrides)
    return json.dumps({k: v for k, v in base.items() if v is not ...})


def test_message_valide(spark):
    assert verdicts(spark, [("ok", message())])["ok"] is None


def test_porte_completude(spark):
    cas = verdicts(spark, [
        ("moisture_nul", message(soil_moisture_pct=None)),
        ("sans_ts", message(ts=...)),
    ])
    assert cas["moisture_nul"] == "completeness:soil_moisture_pct"
    assert cas["sans_ts"].startswith("completeness:")


@pytest.mark.parametrize(("champ", "valeur"), [
    ("soil_moisture_pct", 148.0),
    ("soil_moisture_pct", -12.4),
    ("air_temp_c", -273.0),
    ("air_humidity_pct", 120.0),
    ("soil_raw", 5000),          # au-dela des 12 bits de l'ADC
    ("battery_v", 9.9),
])
def test_porte_bornes(spark, champ, valeur):
    reason = verdicts(spark, [("x", message(**{champ: valeur}))])["x"]
    assert reason == f"range:{champ}"


def test_porte_plausibilite_horodatage_trop_vieux(spark):
    vieux = (dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
             - dt.timedelta(hours=MAX_LATE_HOURS + 2))
    reason = verdicts(spark, [("x", message(ts=vieux.isoformat()))])["x"]
    assert reason == "plausibility:stale_ts"


def test_porte_plausibilite_horodatage_dans_le_futur(spark):
    futur = (dt.datetime(2026, 8, 10, 12, 0, tzinfo=dt.UTC)
             + dt.timedelta(hours=2))
    reason = verdicts(spark, [("x", message(ts=futur.isoformat()))])["x"]
    assert reason == "plausibility:future_ts"


def test_payload_illisible(spark):
    assert verdicts(spark, [("x", "ceci n'est pas du json")])["x"] == \
        "unparseable_payload"


def test_le_verdict_est_deterministe(spark):
    """Meme donnee, meme heure d'ingestion, meme verdict -- toujours.

    La plausibilite est ancree sur `kafka_ts`, pas sur l'horloge murale.
    Rejouer un historique un mois plus tard doit donner exactement les memes
    rejets, sinon le pipeline n'est pas rejouable.
    """
    vieux = dt.datetime(2024, 1, 1, 8, 0, tzinfo=dt.UTC)
    payload = message(ts=vieux.isoformat())
    ingestion = vieux + dt.timedelta(minutes=1)
    assert verdicts(spark, [("x", payload)], ingestion)["x"] is None


def test_la_premiere_regle_qui_echoue_l_emporte(spark):
    """Ordre : completude, puis bornes, puis plausibilite."""
    reason = verdicts(spark, [("x", message(soil_moisture_pct=None,
                                            air_temp_c=999.0))])["x"]
    assert reason == "completeness:soil_moisture_pct"


def test_le_schema_est_declare_et_non_infere():
    """Le contrat de donnees est explicite : neuf colonnes typees."""
    assert len(TELEMETRY_SCHEMA.fields) == 9
    types = {f.name: f.dataType.simpleString() for f in TELEMETRY_SCHEMA.fields}
    assert types["soil_moisture_pct"] == "double"
    assert types["soil_raw"] == "int"
    assert types["ts"] == "string"   # converti ensuite, pour maitriser l'echec
