# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Patient & Claims Analytics
# MAGIC
# MAGIC End-to-end Bronze -> Silver -> Gold demonstration.

# COMMAND ----------

from src.pipeline import create_spark, read_bronze, build_silver, build_gold

spark = create_spark()
patients, claims = read_bronze(spark, "data/raw")

# COMMAND ----------

patients_silver, claims_silver = build_silver(patients, claims)
patients_silver.show()
claims_silver.show()

# COMMAND ----------

gold = build_gold(patients_silver, claims_silver)
for name, df in gold.items():
    print(f"=== {name} ===")
    display(df)
