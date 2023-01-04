import unittest
from datetime import datetime

from odap.feature_factory.no_target_optimizer import replace_pyspark_target_join, replace_sql_target_join


class NoTargetOptimizationTest(unittest.TestCase):
    def test_python_simple(self):
        timestamp = datetime(2020, 12, 12)
        cell = """df.join(target_store, on="client_id", how="inner").filter(f.col("event_date").between(f.col("timestamp") - f.expr("interval 90 days"), f.col("timestamp")))"""

        result = replace_pyspark_target_join(cell, timestamp)
        expected = """df.withColumn("timestamp", f.lit("2020-12-12 00:00:00").cast("timestamp")).filter(f.col("event_date").between(f.col("timestamp") - f.expr("interval 90 days"), f.col("timestamp")))"""

        self.assertEqual(expected, result)

    def test_python_indented(self):
        timestamp = datetime(2020, 12, 12)
        cell = """df.join(
        target_store,
        on="client_id",
        how="inner").filter(
    f.col("event_date").between(
        f.col("timestamp") - f.expr("interval 90 days"), f.col("timestamp")
    )
)"""

        result = replace_pyspark_target_join(cell, timestamp)
        expected = """df.withColumn("timestamp", f.lit("2020-12-12 00:00:00").cast("timestamp")).filter(
    f.col("event_date").between(
        f.col("timestamp") - f.expr("interval 90 days"), f.col("timestamp")
    )
)"""

        self.assertEqual(expected, result)

    def test_sql_simple(self):
        timestamp = datetime(2020, 12, 12)
        cell = """select client_id, timestamp, sum(amount) from db.transactions join target_store using (client_id) where event_date <= timestamp"""

        result = replace_sql_target_join(cell, timestamp)
        expected = """select timestamp("2020-12-12 00:00:00") as timestamp, client_id, timestamp, sum(amount) from db.transactions where event_date <= timestamp"""

        self.assertEqual(expected, result)

    def test_sql_indented(self):
        timestamp = datetime(2020, 12, 12)
        cell = """select
  client_id,
  timestamp,
  sum(amount)
from
  db.transactions
  join target_store using (client_id)
where
  event_date <= timestamp"""

        result = replace_sql_target_join(cell, timestamp)
        expected = """select timestamp("2020-12-12 00:00:00") as timestamp,
  client_id,
  timestamp,
  sum(amount)
from
  db.transactions
  where
  event_date <= timestamp"""

        self.assertEqual(expected, result)


if __name__ == "__main__":
    unittest.main()
