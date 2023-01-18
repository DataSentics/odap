# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

import datetime as dt
from odap.feature_factory import time_windows as tw
from odap.common.test.PySparkTestCase import PySparkTestCase

from functions.core import run_test
from functions.product_web_visits_count import product_agg_features, products

# COMMAND ----------

class ProductWebVisitsCountTest(PySparkTestCase):
    @property
    def target_store(self):
        return self.spark.createDataFrame([
            [_id, dt.datetime(2019, 12, 12), "no target"] for _id in ["1", "2", "3", "4"]
        ], ["customer_id", "timestamp", "target"])
  
    def test_product_agg_features(self):
        time_windows = ["30d", "90d"]

        df = self.spark.createDataFrame([
            ["1", dt.datetime(2019, 11, 12), "Link/to/inveStice"],
            ["1", dt.datetime(2019, 11, 12), "Link/to/inveStice2"],
            ["1", dt.datetime(2019, 11, 12), "Link/to/investice3"],
            ["2", dt.datetime(2019, 11, 12), "Link/to/Hypoteky"],
            ["2", dt.datetime(2019, 11, 12), "Link/to/hypoteky2"],
            ["3", dt.datetime(2019, 11, 12), "Link/to/pujcky"],
            ["4", dt.datetime(2019, 11, 12), "Link/to/site"],
            
        ], ["customer_id", "time_column", "url"])

        wdf = tw.WindowedDataFrame(
            df=df,
            time_column="time_column",
            time_windows=time_windows
        )

        wdf = wdf.join(self.target_store, on="customer_id")
        df = wdf.time_windowed(group_keys=["customer_id", "timestamp"], agg_columns_function=product_agg_features)

        columns = [f"{product}_web_visits_count_in_last_{time_window}" for time_window in time_windows for product in products]

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2019, 12, 12), 3, 0, 0, 3, 0, 0],
                ["2", dt.datetime(2019, 12, 12), 0, 0, 2, 0, 0, 2],
                ["3", dt.datetime(2019, 12, 12), 0, 1, 0, 0, 1, 0],
                ["4", dt.datetime(2019, 12, 12), 0, 0, 0, 0, 0, 0]
            ], ['customer_id', 'timestamp'] + columns)
        
        self.compare_dataframes(df, expected_df, sort_keys=["customer_id"])

# COMMAND ----------

run_test(ProductWebVisitsCountTest)
