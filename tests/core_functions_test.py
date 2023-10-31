# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

import datetime as dt

from pyspark.sql import functions as f

from odap.common.test.PySparkTestCase import PySparkTestCase
from odap.common.test.runner import run_test

# COMMAND ----------

class CountTest(PySparkTestCase):
    @property
    def target_store(self):
        return self.spark.createDataFrame([
            [_id, dt.datetime(2019, 12, 12), "no target"] for _id in ["1", "2", "3", "4"]
        ], ["customer_id", "timestamp", "target"])
  
    def test_count(self):
        df = self.spark.createDataFrame([
            ["1", dt.datetime(2019, 11, 12), "Link/to/inveStice"],
            ["1", dt.datetime(2019, 11, 12), "Link/to/inveStice2"],
            ["1", dt.datetime(2019, 11, 12), "Link/to/investice3"],
            ["2", dt.datetime(2019, 11, 12), "Link/to/Hypoteky"],
            ["2", dt.datetime(2019, 11, 12), "Link/to/hypoteky2"],
            ["3", dt.datetime(2019, 11, 12), "Link/to/pujcky"],
            ["4", dt.datetime(2019, 11, 12), "Link/to/site"],
            
        ], ["customer_id", "time_column", "url"])

        df.groupBy("customer_id", "time_column").agg(f.count("*").alias("count"))

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2019, 12, 12), 3],
                ["2", dt.datetime(2019, 12, 12), 2],
                ["3", dt.datetime(2019, 12, 12), 1],
                ["4", dt.datetime(2019, 12, 12), 1]
            ], ['customer_id', 'timestamp', 'count'])
        
        self.compare_dataframes(df, expected_df, sort_keys=["customer_id"])

# COMMAND ----------

run_test(CountTest)
