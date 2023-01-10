# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

import datetime as dt
from typing import List
from pyspark.sql import functions as f
from odap.feature_factory import time_windows as tw

# COMMAND ----------

dbutils.widgets.text("timestamp", "")
dbutils.widgets.text("target", "")

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

products = ["investice", "pujcky", "hypoteky"]

# COMMAND ----------

time_windows = ["14d", "30d", "90d"]

# COMMAND ----------

wdf_orig = tw.WindowedDataFrame(
    df=spark.read.table("odap_digi_sdm_l2.web_visits"),
    time_column="visit_timestamp",
    time_windows=time_windows,
)

# COMMAND ----------

target_store = spark.read.table("target_store")

# COMMAND ----------

wdf = wdf_orig.join(target_store, on="customer_id").filter(f.col("visit_timestamp") <= f.col("timestamp"))

# COMMAND ----------

# MAGIC %python
# MAGIC metadata = {
# MAGIC     "category": "web_visits",
# MAGIC     "table": "product_features",
# MAGIC     "features": {
# MAGIC         "{product}_web_visits_count_in_last_{time_window}": {
# MAGIC             "description": "Number of {product} web visits in last {time_window}",
# MAGIC             "fillna_with": 0,
# MAGIC         }
# MAGIC     }
# MAGIC }

# COMMAND ----------

def product_agg_features(time_window: str) -> List[tw.WindowedColumn]:    
    return [
        tw.sum_windowed(
            f"{product}_web_visits_count_in_last_{time_window}",
            f.lower("url").contains(product).cast("integer"),
        )
        for product in products
    ]

# COMMAND ----------

df_final = wdf.time_windowed(group_keys=["customer_id", "timestamp"], agg_columns_function=product_agg_features)
# df_final.display()
