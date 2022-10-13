import os
import sys
import re
from base64 import b64decode
from databricks_cli.workspace.api import WorkspaceApi


def get_repository_root_fs_path() -> str:
    return min([path for path in sys.path if path.startswith("/Workspace/Repos")], key=len)


def get_repository_root_api_path() -> str:
    return re.sub("^/Workspace", "", get_repository_root_fs_path())


def get_absolute_path(*paths: str) -> str:
    return os.path.join(get_repository_root_api_path(), *paths)


def get_notebook_content(notebook_path: str, workspace_api: WorkspaceApi) -> str:
    output = workspace_api.client.export_workspace(notebook_path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content)

    return decoded_content.decode("utf-8")
