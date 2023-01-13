import unittest
from pyspark.sql import functions as f

def with_timestamps_no_filter(df, target_store, entity_id, time_column):
    return df.join(target_store, on=entity_id).filter(f.col(time_column) <= f.col("timestamp"))

def run_test(test_case):
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(test_case)
    return unittest.TextTestRunner().run(suite)
