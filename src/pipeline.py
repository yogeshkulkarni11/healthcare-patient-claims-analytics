from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("HealthcarePatientClaimsAnalytics").getOrCreate()


def read_bronze(spark: SparkSession, data_dir: str = "data/raw"):
    patients = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/patients.csv")
    claims = spark.read.option("header", True).option("inferSchema", True).csv(f"{data_dir}/claims.csv")
    return patients, claims


def build_silver(patients: DataFrame, claims: DataFrame):
    patients_silver = (
        patients.dropDuplicates(["patient_id"])
        .filter(F.col("patient_id").isNotNull())
        .filter((F.col("age") > 0) & (F.col("age") < 120))
        .withColumn("age_band", F.when(F.col("age") < 40, "<40").when(F.col("age") < 60, "40-59").otherwise("60+"))
    )
    claims_silver = (
        claims.dropDuplicates(["claim_id"])
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("patient_id").isNotNull())
        .filter(F.col("claim_amount") >= 0)
        .withColumn("claim_date", F.to_date("claim_date"))
        .withColumn("claim_month", F.date_format("claim_date", "yyyy-MM"))
        .withColumn("high_cost_flag", F.when(F.col("claim_amount") >= 50000, 1).otherwise(0))
    )
    return patients_silver, claims_silver


def build_gold(patients: DataFrame, claims: DataFrame):
    joined = claims.join(patients, "patient_id", "left")

    monthly = (
        joined.groupBy("claim_month")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.sum(F.when(F.col("claim_status").isin("Paid", "Approved"), 1).otherwise(0)).alias("successful_claims"),
        )
        .withColumn("success_rate_pct", F.round(F.col("successful_claims") / F.col("claim_count") * 100, 2))
        .orderBy("claim_month")
    )

    provider = (
        joined.groupBy("provider")
        .agg(F.count("claim_id").alias("claim_count"), F.sum("claim_amount").alias("total_claim_amount"), F.avg("claim_amount").alias("avg_claim_amount"))
        .withColumn("provider_rank", F.dense_rank().over(Window.orderBy(F.desc("total_claim_amount"))))
        .orderBy("provider_rank")
    )

    patient = (
        joined.groupBy("patient_id", "age_band", "chronic_condition", "insurance_type")
        .agg(F.count("claim_id").alias("claim_count"), F.sum("claim_amount").alias("total_claim_amount"), F.max("claim_amount").alias("max_claim_amount"))
        .withColumn("patient_cost_rank", F.dense_rank().over(Window.orderBy(F.desc("total_claim_amount"))))
        .orderBy("patient_cost_rank")
    )

    diagnosis = (
        joined.groupBy("diagnosis")
        .agg(F.count("claim_id").alias("claim_count"), F.sum("claim_amount").alias("total_claim_amount"), F.avg("claim_amount").alias("avg_claim_amount"))
        .orderBy(F.desc("total_claim_amount"))
    )

    return {"monthly": monthly, "provider": provider, "patient": patient, "diagnosis": diagnosis}


def run(data_dir: str = "data/raw"):
    spark = create_spark()
    try:
        patients, claims = read_bronze(spark, data_dir)
        patients_silver, claims_silver = build_silver(patients, claims)
        return build_gold(patients_silver, claims_silver)
    finally:
        spark.stop()


if __name__ == "__main__":
    outputs = run()
    for name, df in outputs.items():
        print(f"\n=== GOLD: {name} ===")
        df.show(truncate=False)
