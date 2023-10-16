from odap.feature_factory.context import Context
from odap.feature_factory.dataframes.dataframe_writer import (
    write_features_df,
    write_latest_features,
    write_metadata_df,
)
from odap.feature_factory.tables_validator import validate_feature_store_tables


def orchestrate():
    context = Context()
    feature_tables = list(context.notebook_table_mapping.keys())
    validate_feature_store_tables(feature_tables, context.config)

    write_metadata_df(context.feature_notebooks, context.config)
    write_features_df(context.table_df_mapping, context.config)


def calculate_latest_table():
    context = Context()

    write_latest_features(context.feature_notebooks, context.config)
