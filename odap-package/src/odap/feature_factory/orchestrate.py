from odap.common.config import get_config_namespace, ConfigNamespace
from odap.feature_factory.dataframes.dataframe_writer import (
    write_features_df,
    write_latest_features,
    write_metadata_df,
)
from odap.feature_factory.feature_notebook import create_notebook_table_mapping, load_feature_notebooks
from odap.feature_factory.feature_notebooks_selection import get_list_of_selected_feature_notebooks
from odap.feature_factory.tables_validator import validate_feature_store_tables


def orchestrate():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

    feature_notebooks = load_feature_notebooks(config, get_list_of_selected_feature_notebooks())
    notebook_table_mapping = create_notebook_table_mapping(feature_notebooks)
    feature_tables = list(notebook_table_mapping.keys())
    validate_feature_store_tables(feature_tables, config)

    write_metadata_df(feature_notebooks, config)
    write_features_df(notebook_table_mapping, config)


def calculate_latest_table():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

    feature_notebooks = load_feature_notebooks(config, get_list_of_selected_feature_notebooks())
    write_latest_features(feature_notebooks, config)
