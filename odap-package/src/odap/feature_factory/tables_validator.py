from typing import List
from odap.common.config import Config
from odap.common.tables import hive_table_exists, feature_store_table_exists
from odap.feature_factory.config import (
    get_features_table,
)


def validate_feature_store_tables(feature_tables: List[str], config: Config):
    validate_features_table(feature_tables, config)


def validate_features_table(feature_tables: List[str], config: Config):
    for feature_table in feature_tables:
        features_table = get_features_table(feature_table, config)

        validate_feature_store_and_hive(features_table)


def validate_feature_store_and_hive(full_table_name: str):
    if feature_store_table_exists(full_table_name) and not hive_table_exists(full_table_name):
        raise Exception(f"Table '{full_table_name}' exists in Databricks Feature Store but not in hive")

    if not feature_store_table_exists(full_table_name) and hive_table_exists(full_table_name):
        raise Exception(f"Table {full_table_name} exists in hive but not in Databricks Feature Store")
