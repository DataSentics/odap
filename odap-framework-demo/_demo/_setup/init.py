# Databricks notebook source
import os
from databricks import feature_store

# COMMAND ----------

feature_store_client = feature_store.FeatureStoreClient()

env = os.environ.get("WRITE_ENV")

offline_sdm_db = f"{env}_odap_offline_sdm_l2"
digi_sdm_db = f"{env}_odap_digi_sdm_l2"
account_db = f"{env}_odap_features_account"
customer_db = f"{env}_odap_features_customer"
segments_db = f"{env}_odap_segments"
targets_db = f"{env}_odap_targets"
use_cases_db = f"{env}_odap_use_cases"

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {offline_sdm_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {digi_sdm_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {account_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {customer_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {segments_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {targets_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {use_cases_db} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {offline_sdm_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {digi_sdm_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {account_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {customer_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {segments_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {targets_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {use_cases_db}")

# COMMAND ----------

def drop_feature_store(table: str):
    try:
        feature_store_client.drop_table(table)

    except:
        pass

# COMMAND ----------

drop_feature_store(f"{customer_db}.simple_features")
drop_feature_store(f"{customer_db}.product_features")

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/{env}_odap_features", recurse=True)
dbutils.fs.rm(f"dbfs:/{env}_odap_segments", recurse=True)

# COMMAND ----------

card_transactions = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/card_transactions.parquet")
customer = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/customer.parquet")
web_visits = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/web_visits.parquet")
web_visits_stream = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/web_visits.parquet").limit(0)

target_store = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/target_store.parquet")

account_features = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/account_features.parquet")
account_metadata = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/account_metadata.parquet")

# COMMAND ----------

card_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{offline_sdm_db}.card_transactions")
customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{offline_sdm_db}.customer")
web_visits.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{digi_sdm_db}.web_visits")
web_visits_stream.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{digi_sdm_db}.web_visits_stream")

target_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{targets_db}.targets")

# COMMAND ----------

account_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{account_db}.features_latest")
account_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{account_db}.metadata")
