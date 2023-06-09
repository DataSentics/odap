from typing import List

from databricks_cli.workspace.api import WorkspaceFileInfo

from odap.common.databricks import get_workspace_api
from odap.common.widgets import get_widget_value
from odap.feature_factory import const
from odap.feature_factory.feature_notebook import get_feature_notebooks_info
from odap.feature_factory.exceptions import WidgetException


def get_list_of_selected_feature_notebooks(feature_dir: str) -> List[WorkspaceFileInfo]:
    feature_notebooks_str = get_widget_value(const.FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api(), feature_dir)

    if feature_notebooks_str == const.ALL_FEATURES:
        return feature_notebooks

    feature_notebooks_list = feature_notebooks_str.split(",")

    feature_notebooks = [
        feature_notebook
        for feature_notebook in feature_notebooks
        if feature_notebook.basename in feature_notebooks_list
    ]

    verify_feature_notebooks(feature_notebooks_list, feature_notebooks)

    return feature_notebooks


def verify_feature_notebooks(feature_notebooks_list: List[str], feature_notebooks: List[WorkspaceFileInfo]):
    check_all_features_is_not_present(feature_notebooks_list)

    check_for_missing_feature_notebooks(feature_notebooks_list, feature_notebooks)

    check_for_duplicate_notebook_names(feature_notebooks_list)


def check_all_features_is_not_present(feature_notebooks_list):
    if const.ALL_FEATURES in feature_notebooks_list:
        raise WidgetException(
            f"`{const.ALL_FEATURES}` together with selected notebooks is not a valid option. Please select "
            f"either `{const.ALL_FEATURES}` only or a subset of notebooks"
        )


def check_for_missing_feature_notebooks(feature_notebooks_list: List[str], feature_notebooks: List[WorkspaceFileInfo]):
    if len(feature_notebooks_list) == 1 and feature_notebooks_list[0] == "":
        raise WidgetException("No feature notebooks were selected")

    missing_feature_notebooks = set(feature_notebooks_list) - set(notebook.basename for notebook in feature_notebooks)

    if missing_feature_notebooks:
        raise WidgetException(f"Following feature notebooks were not found: {missing_feature_notebooks}")


def check_for_duplicate_notebook_names(feature_notebooks_list: List[str]):
    duplicate_notebook_names = set(
        notebook for notebook in feature_notebooks_list if feature_notebooks_list.count(notebook) > 1
    )

    if duplicate_notebook_names:
        raise WidgetException(
            f"Following feature notebook names are present more than once: {duplicate_notebook_names}"
        )
