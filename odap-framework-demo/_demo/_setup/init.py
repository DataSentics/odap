# Databricks notebook source

import os
from databricks import feature_store

# COMMAND ----------

feature_store_client = feature_store.FeatureStoreClient()
env = os.environ.get("WRITE_ENV")
account_db = f'{env}_odap_features_account'
customer_db = f'{env}_odap_features_customer'

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS odap_offline_sdm_l2 CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_digi_sdm_l2 CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {account_db} CASCADE")
spark.sql(f"DROP DATABASE IF EXISTS {customer_db} CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_segments CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_targets CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_use_cases CASCADE")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS odap_offline_sdm_l2")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_digi_sdm_l2")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {account_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {customer_db}")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_segments")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_targets")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_use_cases")

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

dbutils.fs.rm(f"dbfs:/{env}/odap_features", recurse=True)
dbutils.fs.rm("dbfs:/odap_segments", recurse=True)

# COMMAND ----------

card_transactions = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/card_transactions.parquet")
customer = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/customer.parquet")
web_visits = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/web_visits.parquet")
web_visits_stream = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/web_visits.parquet").limit(0)

target_store = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/target_store.parquet")

account_features = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/account_features.parquet")
account_metadata = spark.read.format("parquet").load(f"file://{os.getcwd()}/../_data/account_metadata.parquet")

# COMMAND ----------

card_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_offline_sdm_l2.card_transactions")
customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_offline_sdm_l2.customer")
web_visits.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_digi_sdm_l2.web_visits")
web_visits_stream.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_digi_sdm_l2.web_visits_stream")

target_store.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_targets.targets")

# COMMAND ----------

account_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{account_db}.features_latest")
account_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{account_db}.metadata")
