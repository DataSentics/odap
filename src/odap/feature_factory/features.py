from typing import List
from databricks_cli.workspace.api import WorkspaceApi
from odap.common.utils import get_absolute_path


def get_features_paths(workspace_api: WorkspaceApi) -> List[str]:
    features_path = get_absolute_path("features")

    return [obj.path for obj in workspace_api.list_objects(features_path)]
