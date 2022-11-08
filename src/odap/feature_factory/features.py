from typing import List
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.workspace.api import WorkspaceFileInfo
from odap.common.utils import get_absolute_path
from odap.common.utils import list_notebooks


def get_feature_notebooks_info(workspace_api: WorkspaceApi) -> List[WorkspaceFileInfo]:
    features_path = get_absolute_path("features")

    return list_notebooks(features_path, workspace_api)
