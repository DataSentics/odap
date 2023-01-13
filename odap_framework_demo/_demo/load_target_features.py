# Databricks notebook source
from databricks.feature_store import FeatureLookup, FeatureStoreClient

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text target default "first_mortgage_payment";
# MAGIC create widget text timestamp default "2020-12-12";
# MAGIC create widget text timeshift default "0"

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

fs = FeatureStoreClient()

# COMMAND ----------

feature_lookups = [
    FeatureLookup(
        table_name="odap_features_customer.product_features",
        feature_names=[
            "investice_web_visits_count_in_last_14d",
            "pujcky_web_visits_count_in_last_14d",
        ],
        lookup_key="customer_id",
        timestamp_lookup_key="timestamp",
    ),
    FeatureLookup(
        table_name="odap_features_customer.simple_features",
        feature_names=[
            "customer_email",
        ],
        lookup_key="customer_id",
        timestamp_lookup_key="timestamp",
    )
]

# COMMAND ----------

training_set = fs.create_training_set(
    spark.table("target_store"),
    feature_lookups=feature_lookups,
    label="target",
)
training_df = training_set.load_df()

# COMMAND ----------

training_df.display()
