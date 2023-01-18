import datetime as dt
import unittest

from odap.common.test.PySparkTestCase import PySparkTestCase
from odap.feature_factory.dataframes.dataframe_creator import join_dataframes


class LatestDataFrameTest(PySparkTestCase):
    def test_latest_df(self):
        df_1 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 12, 12), 10, "a"],
                ["2", dt.datetime(2020, 12, 12), 7, "b"],
                ["3", dt.datetime(2020, 12, 12), 2, "c"],
                ["4", dt.datetime(2020, 12, 12), 4, "a"],
            ],
            ["id", "timestamp", "f1", "f2"],
        )

        df_2 = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 12, 11), 11, "d"],
                ["2", dt.datetime(2020, 12, 11), 20, "e"],
                ["3", dt.datetime(2020, 12, 11), 35, "f"],
            ],
            ["id", "timestamp", "f3", "f4"],
        )

        latest_df = join_dataframes([df_1, df_2], ["id"])

        expected_df = self.spark.createDataFrame(
            [
                ["1", dt.datetime(2020, 12, 12), 10, "a", 11, "d"],
                ["2", dt.datetime(2020, 12, 12), 7, "b", 20, "e"],
                ["3", dt.datetime(2020, 12, 12), 2, "c", 35, "f"],
                ["4", dt.datetime(2020, 12, 12), 4, "a", None, None],
            ],
            ["id", "timestamp", "f1", "f2", "f3", "f4"],
        )

        self.compare_dataframes(expected_df, latest_df, ["id"])


if __name__ == "__main__":
    unittest.main()
