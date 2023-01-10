from typing import List

from databricks_cli.workspace.api import WorkspaceFileInfo

from odap.common.databricks import get_workspace_api
from odap.common.widgets import get_widget_value
from odap.feature_factory import const
from odap.feature_factory.feature_notebook import get_feature_notebooks_info


def get_list_of_selected_feature_notebooks() -> List[WorkspaceFileInfo]:
    feature_notebooks_str = get_widget_value(const.FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    if feature_notebooks_str == const.ALL_FEATURES:
        return feature_notebooks

    feature_notebooks_list = feature_notebooks_str.split(",")

    if const.ALL_FEATURES in feature_notebooks_list:
        raise Exception(
            f"`{const.ALL_FEATURES}` together with selected notebooks is not a valid option. Please select "
            f"either `{const.ALL_FEATURES}` only or a subset of notebooks"
        )

    return [
        feature_notebook
        for feature_notebook in feature_notebooks
        if feature_notebook.basename in feature_notebooks_list
    ]
