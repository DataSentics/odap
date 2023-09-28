from typing import Dict, Any
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.feature_factory.config import get_feature_dir, get_repository
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
    feature_dir_init = get_feature_dir(config)
    databricks_repo = get_repository(config)

    for repo_paths in databricks_repo:
        
        feature_dir = f"{repo_paths['path']}/{feature_dir_init}"
        prefix = repo_paths['prefix']

        process_feature_dir(config,feature_dir, prefix)

def process_feature_dir(config: Dict, feature_dir: str, prefix: str) -> Any:
    feature_notebooks = load_feature_notebooks(config, get_list_of_selected_feature_notebooks(feature_dir))
   
    notebook_table_mapping = create_notebook_table_mapping(feature_notebooks)
    feature_tables = list(notebook_table_mapping.keys())
    validate_feature_store_tables(feature_tables, config)
   
    write_metadata_df(feature_notebooks, config)
    write_features_df(notebook_table_mapping, config, prefix)


def calculate_latest_table():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    feature_dir_init = get_feature_dir(config)
    databricks_repo = get_repository(config)

    for repo_paths in databricks_repo:
        feature_dir = f"{repo_paths['path']}/{feature_dir_init}"
        prefix = repo_paths['prefix']

        feature_notebooks = load_feature_notebooks(config, get_list_of_selected_feature_notebooks(feature_dir))
        write_latest_features(feature_notebooks, config, prefix)