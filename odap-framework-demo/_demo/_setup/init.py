# Databricks notebook source
import os
from databricks import feature_store

# COMMAND ----------

feature_store_client = feature_store.FeatureStoreClient()

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS odap_offline_sdm_l2 CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_digi_sdm_l2 CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_features_account CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_features_customer CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_segments CASCADE")
spark.sql("DROP DATABASE IF EXISTS odap_targets CASCADE")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS odap_offline_sdm_l2")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_digi_sdm_l2")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_features_account")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_features_customer")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_segments")
spark.sql("CREATE DATABASE IF NOT EXISTS odap_targets")

# COMMAND ----------

def drop_feature_store(table: str):
    try:
        feature_store_client.drop_table(table)

    except:
        pass

# COMMAND ----------

drop_feature_store("odap_features_customer.simple_features")
drop_feature_store("odap_features_customer.product_features")

# COMMAND ----------

dbutils.fs.rm("dbfs:/odap_features", recurse=True)
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

account_features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_features_account.features_account_latest")
account_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("odap_features_account.metadata")
