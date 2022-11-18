from typing import List

from databricks_cli.workspace.api import WorkspaceFileInfo

from odap.common.databricks import get_workspace_api
from odap.common.widgets import get_widget_value
from odap.feature_factory import const
from odap.feature_factory.feature_notebook import get_feature_notebooks_info

ALL = "<all>"
FEATURE_WIDGET = "feature"


def get_list_of_selected_feature_notebooks() -> List[WorkspaceFileInfo]:
    feature_notebook_name = get_widget_value(const.FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    if feature_notebook_name == const.ALL_FEATURES:
        return feature_notebooks

    return [
        feature_notebook for feature_notebook in feature_notebooks if feature_notebook.basename == feature_notebook_name
    ]
