from pyspark.sql import DataFrame

from odap.common.config import ConfigNamespace, get_config_namespace
from odap.common.logger import logger
from odap.common.widgets import get_widget_value
from odap.feature_factory import const
from odap.feature_factory.config import (
    get_entity_primary_key,
)
from odap.feature_factory.dataframes.dataframe_creator import create_features_df, create_metadata_df
from odap.feature_factory.feature_notebook import get_feature_notebooks_from_dirs


def dry_run():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    feature_notebooks = get_feature_notebooks_from_dirs(config)

    entity_primary_key = get_entity_primary_key(config)

    features_df = create_features_df(feature_notebooks, entity_primary_key)
    metadata_df = create_metadata_df(feature_notebooks)

    logger.info("Success. No errors found!")

    display_dataframes(features_df, metadata_df)


def display_dataframes(features_df: DataFrame, metadata_df: DataFrame):
    display_widget_value = get_widget_value(const.DISPLAY_WIDGET)

    if const.DISPLAY_FEATURES in display_widget_value:
        display_features_df(features_df)

    if const.DISPLAY_METADATA in display_widget_value:
        display_metadata_df(metadata_df)


def display_metadata_df(metadata_df: DataFrame):
    print("\nMetadata DataFrame:")
    metadata_df.display()  # pyre-ignore[29]


def display_features_df(final_df: DataFrame):
    print("\nFeatures DataFrame:")
    final_df.display()  # pyre-ignore[29]
