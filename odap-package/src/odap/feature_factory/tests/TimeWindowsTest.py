import unittest
from datetime import datetime
import time
import datetime as dt

from pyspark.sql import functions as f

from odap.common.test.PySparkTestCase import PySparkTestCase
from odap.feature_factory.time_windows import (
    WindowedDataFrame,
    count_windowed,
    sum_windowed,
    is_past_time_window,
    with_time_windows,
)


def get_unix_timestamp(date_str: str):
    return time.mktime(datetime.strptime(date_str, "%Y-%m-%d").timetuple())


class WindowedFeaturesTest(PySparkTestCase):
    def test_past_time_windows(self):
        timestamp = get_unix_timestamp("2021-11-18")
        arg_timestamp = get_unix_timestamp("2021-10-17")

        self.assertFalse(is_past_time_window(timestamp, arg_timestamp, "30d"))  # noqa
        self.assertTrue(is_past_time_window(timestamp, arg_timestamp, "60d"))  # noqa
        self.assertTrue(is_past_time_window(timestamp, arg_timestamp, "90d"))  # noqa

    def test_wrong_order_time_windows(self):
        arg_timestamp = get_unix_timestamp("2021-11-18")
        timestamp = get_unix_timestamp("2021-10-17")

        self.assertFalse(is_past_time_window(timestamp, arg_timestamp, "30d"))  # noqa
        self.assertFalse(is_past_time_window(timestamp, arg_timestamp, "60d"))  # noqa
        self.assertFalse(is_past_time_window(timestamp, arg_timestamp, "90d"))  # noqa

    def test_simple_time_windows(self):
        timestamp = dt.datetime(2020, 2, 16)

        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 2, 15), timestamp],
                ["2", dt.datetime(2020, 2, 1), timestamp],
                ["3", dt.datetime(2020, 1, 15), timestamp],
            ],
            ["id", "date", "timestamp"],
        )
        result = with_time_windows(df_1, "timestamp", "date", ["14d", "30d"])
        reference = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 2, 15), timestamp, True, True],
                ["2", dt.datetime(2020, 2, 1), timestamp, False, True],
                ["3", dt.datetime(2020, 1, 15), timestamp, False, False],
            ],
            ["id", "date", "timestamp", "is_time_window_14d", "is_time_window_30d"],
        )

        self.assertEqual(reference.collect(), result.collect())

    def test_future_time_windows(self):
        timestamp = dt.datetime(2020, 2, 16)
        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 2, 17), timestamp],
                ["2", dt.datetime(2020, 2, 1), timestamp],
                ["3", dt.datetime(2020, 1, 15), timestamp],
            ],
            ["id", "date", "timestamp"],
        )

        result = with_time_windows(df_1, "timestamp", "date", ["14d", "30d"])
        reference = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 2, 17), timestamp, False, False],
                ["2", dt.datetime(2020, 2, 1), timestamp, False, True],
                ["3", dt.datetime(2020, 1, 15), timestamp, False, False],
            ],
            ["id", "date", "timestamp", "is_time_window_14d", "is_time_window_30d"],
        )

        self.assertEqual(reference.collect(), result.collect())

    def test_agg_time_windows(self):
        timestamp = dt.datetime(2020, 2, 16)
        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 2, 15), 10, timestamp],
                ["1", dt.datetime(2020, 2, 1), 7, timestamp],
                ["1", dt.datetime(2020, 1, 18), 2, timestamp],
                ["2", dt.datetime(2020, 1, 17), 4, timestamp],
                ["2", dt.datetime(2020, 1, 15), 12, timestamp],
            ],
            ["id", "date", "amount", "timestamp"],
        )

        wdf = WindowedDataFrame(df_1, "date", ["14d", "30d"])

        def agg_features(time_window: str):
            return [
                count_windowed(
                    f"count_{time_window}",
                    f.col("amount"),
                ),
                sum_windowed(
                    f"sum_{time_window}",
                    f.col("amount"),
                ),
            ]

        result = wdf.time_windowed(["id", "timestamp"], agg_features).sort("id")
        reference = self.spark.createDataFrame(
            [
                ["1", timestamp, 1, 10, 3, 19],
                ["2", timestamp, 0, None, 1, 4],
            ],
            ["id", "timestamp", "count_14d", "sum_14d", "count_30d", "sum_30d"],
        )

        self.assertEqual(reference.collect(), result.collect())


if __name__ == "__main__":
    unittest.main()
