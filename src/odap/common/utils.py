import os
import sys
import re
from base64 import b64decode
from pyspark.dbutils import DBUtils
from databricks_cli.workspace.api import WorkspaceApi


def get_absolute_path(*paths: str) -> str:
    workspace_root = min([path for path in sys.path if path.startswith("/Workspace/Repos")], key=len)
    repos_root = re.sub("^/Workspace", "", workspace_root)

    return os.path.join(repos_root, *paths)


def get_notebook_content(notebook_path: str, workspace_api: WorkspaceApi) -> str:
    output = workspace_api.client.export_workspace(notebook_path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content)

    return decoded_content.decode("utf-8")
