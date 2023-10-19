from odap.common.databricks import resolve_dbutils, get_workspace_api
from odap.common.utils import get_notebook_name
from odap.feature_factory import const
from odap.feature_factory.config import Config, get_feature_source_dir, get_feature_source_prefix
from odap.feature_factory.feature_notebooks_selection import get_feature_notebooks_info
from odap.feature_factory.utils import widget_prefix


def create_notebooks_widget():
    dbutils = resolve_dbutils()

    config = Config.load_feature_factory_config()
    feature_sources = config.get_feature_sources()
    feature_notebooks_all = []

    for feature_source in feature_sources:
        features_dir = get_feature_source_dir(feature_source)
        prefix = get_feature_source_prefix(feature_source)

        feature_notebooks = [
            f"{widget_prefix(prefix)}{get_notebook_name(notebook_info.path)}"
            for notebook_info in get_feature_notebooks_info(get_workspace_api(), features_dir)
        ]
        feature_notebooks_all.extend(feature_notebooks)

    dbutils.widgets.multiselect(const.FEATURE_WIDGET, const.ALL_FEATURES, [const.ALL_FEATURES] + feature_notebooks_all)


def create_dry_run_widgets():
    dbutils = resolve_dbutils()

    create_notebooks_widget()

    dbutils.widgets.multiselect(
        const.DISPLAY_WIDGET, const.DISPLAY_METADATA, choices=[const.DISPLAY_METADATA, const.DISPLAY_FEATURES]
    )
