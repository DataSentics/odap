# Databricks notebook source
import unittest
import datetime as dt
from odap_framework_demo.functions.core import with_timestamps_no_filter, run_test

# COMMAND ----------

class CoreFunctionsTest(unittest.TestCase):
    @property
    def target_store(self):
        return spark.createDataFrame([
            ["1", dt.datetime(2020, 12, 12), "no target"],
            ["1", dt.datetime(2019, 12, 12), "no target"],
            ["2", dt.datetime(2020, 12, 12), "no target"],
            ["2", dt.datetime(2018, 12, 12), "no target"],
            ["3", dt.datetime(2018, 12, 12), "no target"],
            ["4", dt.datetime(2018, 12, 12), "no target"],
            
        ], ["customer_id", "timestamp", "target"])

    def test_with_timestamps_no_filter(self):
        df = spark.createDataFrame([
            ["1", dt.datetime(2019, 11, 12)],
            ["2", dt.datetime(2019, 11, 12)],
            ["3", dt.datetime(2019, 11, 12)],
            ["4", dt.datetime(2019, 11, 12)],
            
        ], ["customer_id", "time_column"])
        
        df_with_timestamps = with_timestamps_no_filter(
            df=df,
            target_store=self.target_store,
            entity_id="customer_id",
            time_column="time_column"
        )
        
        expected_df = spark.createDataFrame([
            ["1", dt.datetime(2019, 11, 12), dt.datetime(2020, 12, 12), "no target"],
            ["1", dt.datetime(2019, 11, 12), dt.datetime(2019, 12, 12), "no target"],
            ["2", dt.datetime(2019, 11, 12), dt.datetime(2020, 12, 12), "no target"],
        ], ["customer_id", "time_column", "timestamp", "target"])
        
        self.assertEqual(df_with_timestamps.collect(), expected_df.collect())


# COMMAND ----------

run_test(CoreFunctionsTest)
