from pyspark.sql import functions as F
from src.pipeline import build_silver, build_gold


def test_silver_derives_age_band_and_high_cost(spark):
    patients = spark.createDataFrame([("P1", "F", 65, "Pune", "Private", "Diabetes")], ["patient_id", "gender", "age", "city", "insurance_type", "chronic_condition"])
    claims = spark.createDataFrame([("C1", "P1", "2026-01-01", "Provider", "Diabetes", "Hospitalization", 60000.0, "Paid")], ["claim_id", "patient_id", "claim_date", "provider", "diagnosis", "procedure", "claim_amount", "claim_status"])
    p, c = build_silver(patients, claims)
    assert p.first()["age_band"] == "60+"
    assert c.first()["high_cost_flag"] == 1


def test_gold_patient_aggregation(spark):
    patients = spark.createDataFrame([("P1", "F", 45, "Pune", "Private", "None")], ["patient_id", "gender", "age", "city", "insurance_type", "chronic_condition"])
    claims = spark.createDataFrame([
        ("C1", "P1", "2026-01-01", "Provider", "Flu", "Consultation", 1000.0, "Paid"),
        ("C2", "P1", "2026-01-05", "Provider", "Flu", "Lab Test", 2000.0, "Approved"),
    ], ["claim_id", "patient_id", "claim_date", "provider", "diagnosis", "procedure", "claim_amount", "claim_status"])
    p, c = build_silver(patients, claims)
    gold = build_gold(p, c)["patient"].collect()[0]
    assert gold["claim_count"] == 2
    assert gold["total_claim_amount"] == 3000.0
