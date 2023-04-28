from odap.common.config import get_config_namespace, ConfigNamespace
from odap.common.databricks import resolve_dbutils, get_workspace_api
from odap.common.utils import get_notebook_name
from odap.feature_factory import const
from odap.feature_factory.config import get_feature_dir
from odap.feature_factory.feature_notebook import get_feature_notebooks_info


def create_notebooks_widget():
    dbutils = resolve_dbutils()

    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    feature_dir = get_feature_dir(config)

    features = [
        get_notebook_name(notebook_info.path)
        for notebook_info in get_feature_notebooks_info(get_workspace_api(), feature_dir)
    ]

    dbutils.widgets.multiselect(const.FEATURE_WIDGET, const.ALL_FEATURES, [const.ALL_FEATURES] + features)


def create_dry_run_widgets():
    dbutils = resolve_dbutils()

    create_notebooks_widget()

    dbutils.widgets.multiselect(
        const.DISPLAY_WIDGET, const.DISPLAY_METADATA, choices=[const.DISPLAY_METADATA, const.DISPLAY_FEATURES]
    )
