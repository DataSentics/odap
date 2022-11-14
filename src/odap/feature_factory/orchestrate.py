from odap.common.config import get_config_namespace, ConfigNamespace
from odap.common.databricks import get_workspace_api
from odap.feature_factory.dataframes.dataframe_writer import (
    write_features_df,
    write_latest_features,
    write_metadata_df,
)
from odap.feature_factory.feature_notebook import get_feature_notebooks_info, load_feature_notebooks


def orchestrate():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

    feature_notebooks = load_feature_notebooks(config, get_feature_notebooks_info(get_workspace_api()))

    write_metadata_df(feature_notebooks, config)
    write_features_df(feature_notebooks, config)

    write_latest_features(config)
