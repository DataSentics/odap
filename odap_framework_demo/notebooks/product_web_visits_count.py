# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

import os

from pyspark.sql import functions as f

# COMMAND ----------

dbutils.widgets.text("timestamp", "2020-12-12")
dbutils.widgets.text("target", "no target")

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

time_windows = [14, 30, 90]
products = ["investice", "pujcky", "hypoteky"]

# COMMAND ----------

df_web_visits = spark.read.table(
    f"{os.environ['READ_ENV']}.odap_digi_sdm_l2.web_visits"
)
target_store = spark.read.table("target_store")

df = df_web_visits.join(target_store, on="customer_id").filter(
    f.col("visit_timestamp") <= f.col("timestamp")
)

# COMMAND ----------

def is_in_time_window(time_column, window_size):
    return time_column.between(
        time_column - f.lit(window_size).cast(f"interval day"), f.col("timestamp")
    )

# COMMAND ----------

df_final = df.groupBy(["customer_id", "timestamp"]).agg(
    *[
        f.sum(
            f.when(
                is_in_time_window(f.col("visit_timestamp"), time_window),
                f.lower("url").contains(product).cast("integer"),
            )
        ).alias(
            f"{product}_web_visits_count_in_last_{time_window}d",
        )
        for product in products
        for time_window in time_windows
    ],
)
# df_final.display()

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
