import os
from typing import List
from base64 import b64decode
from pyspark.dbutils import DBUtils
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.sdk.api_client import ApiClient

from feature_factory.databricks_context import get_host, get_token


def get_features_folder_path(dbutils: DBUtils) -> str:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    notebook_folder_path = notebook_path.rpartition("/")[0]

    return os.path.join(notebook_folder_path, "features")


def get_features_paths(dbutils: DBUtils, workspace_api: WorkspaceApi) -> List[str]:
    features_path = get_features_folder_path(dbutils)

    return [obj.path for obj in workspace_api.list_objects(features_path)]


def get_workspace_api(dbutils: DBUtils) -> WorkspaceApi:
    api_client = ApiClient(host=get_host(dbutils), token=get_token(dbutils))
    return WorkspaceApi(api_client)


def get_feature_content(workspace_api: WorkspaceApi, feature_path: str) -> str:
    output = workspace_api.client.export_workspace(feature_path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content)
    return decoded_content.decode("utf-8")
