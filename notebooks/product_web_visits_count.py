# Databricks notebook source
# MAGIC %md
# MAGIC # Web product visits features

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

import os
from pyspark.sql import functions as f

from odap.feature_factory.imports import get_param

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("timestamp", "2020-12-12")
dbutils.widgets.text("target", "no target")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init target store

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature configuration

# COMMAND ----------

params = get_param("web_features")

# COMMAND ----------

time_windows = params["time_windows"]  # [14, 30, 90]
products = params["products"]  # ["investice", "pujcky", "hypoteky"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load web visits data

# COMMAND ----------

df_web_visits = spark.read.table(
    f"{os.environ['READ_ENV']}.odap_digi_sdm_l2.web_visits"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load target store

# COMMAND ----------

target_store = spark.read.table("target_store")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join target store with source data
# MAGIC Filter out data later than timestamp for historical calculation

# COMMAND ----------

df = df_web_visits.join(target_store, on="customer_id").filter(
    f.col("visit_timestamp") <= f.col("timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper function for time window calculation
# MAGIC Boolean column to see if row is in a given time window

# COMMAND ----------

def is_in_time_window(time_column, window_size):
    return time_column.between(
        f.col("timestamp") - f.lit(window_size).cast(f"interval day"),
        f.col("timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate features

# COMMAND ----------

df_final = df.groupBy(["customer_id", "timestamp"]).agg(
    *[
        f.sum(
            f.when(
                is_in_time_window(f.col("visit_timestamp"), time_window),
                f.lower("url").contains(product).cast("integer"),
            ).otherwise(None)
        ).alias(
            f"{product}_web_visits_count_in_last_{time_window}d",
        )
        for product in products
        for time_window in time_windows
    ],
)
# df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define metadata

# COMMAND ----------

# MAGIC %python
# MAGIC metadata = {
# MAGIC     "category": "web_visits",
# MAGIC     "table": "product_features",
# MAGIC     "owner": "lukas.langr@datasentics.com",
# MAGIC     "features": {
# MAGIC         "{product}_web_visits_count_in_last_{time_window}": {
# MAGIC             "description": "Number of {product} web visits in last {time_window}",
# MAGIC             "fillna_with": 0,
# MAGIC         }
# MAGIC     }
# MAGIC }
