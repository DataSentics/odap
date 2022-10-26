import os
import sys
import re
import importlib
from base64 import b64decode
from typing import List
from databricks_cli.workspace.api import WorkspaceApi

SQL_CELL_DIVIDER = "-- COMMAND ----------"
PYTHON_CELL_DIVIDER = "# COMMAND ----------"


def get_repository_root_fs_path() -> str:
    return min((path for path in sys.path if path.startswith("/Workspace/Repos")), key=len)


def get_repository_root_api_path() -> str:
    return re.sub("^/Workspace", "", get_repository_root_fs_path())


def get_relative_path(path: str):
    return os.path.relpath(path, get_repository_root_api_path())


def get_notebook_name(path: str):
    return os.path.split(path)[-1]


def get_absolute_path(*paths: str) -> str:
    return os.path.join(get_repository_root_api_path(), *paths)


def get_notebook_cells(notebook_path: str, workspace_api: WorkspaceApi) -> List[str]:
    output = workspace_api.client.export_workspace(notebook_path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content).decode("utf-8")

    return split_notebok_to_cells(decoded_content)


def split_notebok_to_cells(notebook_content):
    if SQL_CELL_DIVIDER in notebook_content:
        return notebook_content.split(SQL_CELL_DIVIDER)

    return notebook_content.split(PYTHON_CELL_DIVIDER)


def join_python_notebook_cells(cells: List[str]) -> str:
    return PYTHON_CELL_DIVIDER.join(cells)


def import_file(module_name: str, file_path: str):
    module_name = f"odap_exporter_{module_name}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)  # pyre-ignore[6]
    sys.modules[spec.name] = module  # pyre-ignore[16]
    spec.loader.exec_module(module)  # pyre-ignore[16]
    return importlib.import_module(module_name)
