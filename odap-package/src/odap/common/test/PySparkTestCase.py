import unittest
import warnings
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.getActiveSession()
            or SparkSession.builder.master("local[1]").appName("PySparkTest").getOrCreate()
        )
        cls.sc = cls.spark.sparkContext  # noqa

        warnings.filterwarnings("ignore", category=RuntimeWarning)
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def compare_dataframes(self, df1: DataFrame, df2: DataFrame, sort_keys: List[str]):
        df1_columns = sorted(df1.columns)
        df2_columns = sorted(df2.columns)

        df1 = df1.orderBy(*sort_keys).select(*df1_columns)
        df2 = df2.orderBy(*sort_keys).select(*df2_columns)

        self.assertEqual(df1.collect(), df2.collect())

    # pylint: disable=too-many-locals,too-many-statements
    def delta_merge(self, target: DataFrame, source: DataFrame, primary_keys: List[str]):  # noqa
        target_columns = target.columns
        source_columns = source.columns
        all_columns = list(set(target_columns) | set(source_columns))
        duplicate_columns = list((set(target_columns) & set(source_columns)) - set(primary_keys))
        unique_columns = list(set(all_columns) - set(primary_keys) - set(duplicate_columns))

        source_selection = []
        target_selection = []

        for col in all_columns:
            if col in unique_columns:
                source_selection.append(col)
                target_selection.append(col)

            if col in duplicate_columns:
                source_selection.append(f.col(f"source.{col}").alias(col))
                target_selection.append(f.col(f"target.{col}").alias(col))

        common_pks = target.select(primary_keys).intersect(source.select(primary_keys))
        target_only_pks = target.select(primary_keys).exceptAll(source.select(primary_keys))
        source_only_pks = source.select(primary_keys).exceptAll(target.select(primary_keys))

        joined_df = target.alias("target").join(source.alias("source"), on=primary_keys, how="outer")
        source_part = joined_df.join(common_pks.union(source_only_pks), on=primary_keys).select(
            *primary_keys, *source_selection
        )
        target_part = joined_df.join(target_only_pks, on=primary_keys).select(*primary_keys, *target_selection)

        return source_part.union(target_part).select(*all_columns)
