from typing import List
from odap.common.config import Config
from odap.common.tables import hive_table_exists, feature_store_table_exists, table_path_exists, get_table_path
from odap.feature_factory.exceptions import TableValidationError
from odap.feature_factory.config import (
    get_features_table,
    get_metadata_table,
    get_features_table_path,
    get_metadata_table_path,
)


def validate_feature_store_tables(feature_tables: List[str], config: Config):
    validate_features_table(feature_tables, config)
    validate_metadata_table(config)


def validate_features_table(feature_tables: List[str], config: Config):
    for feature_table in feature_tables:
        features_table = get_features_table(feature_table, config)
        features_path = get_features_table_path(feature_table, config)

        validate_feature_store_and_hive(features_table)
        validate_hive_and_path(features_table, features_path)


def validate_metadata_table(config: Config):
    metadata_table = get_metadata_table(config)
    metadata_path = get_metadata_table_path(config)

    validate_hive_and_path(metadata_table, metadata_path)


def validate_feature_store_and_hive(full_table_name: str):
    if feature_store_table_exists(full_table_name) and not hive_table_exists(full_table_name):
        raise Exception(f"Table '{full_table_name}' exists in Databricks Feature Store but not in hive")

    if not feature_store_table_exists(full_table_name) and hive_table_exists(full_table_name):
        raise Exception(f"Table {full_table_name} exists in hive but not in Databricks Feature Store")


def validate_hive_and_path(full_table_name: str, path: str):
    if hive_table_exists(full_table_name) and not table_path_exists(path):
        raise TableValidationError(f"Table '{full_table_name}' exists in hive but not in path '{path}'")

    if not hive_table_exists(full_table_name) and table_path_exists(path):
        raise TableValidationError(f"Table '{full_table_name}' doesn't exists in hive but exists in path '{path}'")

    if hive_table_exists(full_table_name) and get_table_path(full_table_name) != path:
        raise TableValidationError(
            f"Table '{full_table_name}' path '{get_table_path(full_table_name)}' mismatch with path '{path}' specified in config"
        )
