# Databricks notebook source
from pyspark.sql import functions as f
from models.ai_user_interested_in_mortgage import AiUserInterestedInMortgageModel

# COMMAND ----------

feature_store = spark.read.table("odap_features.features_customer")

# COMMAND ----------

model = AiUserInterestedInMortgageModel()

# COMMAND ----------

df = (
    spark
    .readStream
    .option("startingVersion", "latest")
    .table("odap_digi_sdm_l2.web_visits_stream")
    .select("customer_id", "visit_timestamp", "url", f.window("visit_timestamp", "24 hours"))
    .withWatermark("visit_timestamp", "24 hours")
    .dropDuplicates(["customer_id", "window"])
    .join(feature_store, on=["customer_id"])
)

# COMMAND ----------

df = (
    model
    .transform(df)
    .filter(f.col("url").contains("hypoteky"))
    .filter(f.col("ai_user_interested_in_mortgage_estimation") >= 0.8)
)

# COMMAND ----------

df.display()
