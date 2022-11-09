from typing import List
from base64 import b64decode
from databricks_cli.workspace.api import WorkspaceApi
from odap.feature_factory.metadata import METADATA_HEADER

SQL_CELL_DIVIDER = "-- COMMAND ----------"
PYTHON_CELL_DIVIDER = "# COMMAND ----------"


def get_notebook_cells(notebook_path: str, workspace_api: WorkspaceApi) -> List[str]:
    output = workspace_api.client.export_workspace(notebook_path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content).decode("utf-8")

    return split_notebok_to_cells(decoded_content)


def get_notebook_language(notebook_path: str, workspace_api: WorkspaceApi) -> str:
    return workspace_api.get_status(notebook_path).language


def split_notebok_to_cells(notebook_content) -> List[str]:
    if SQL_CELL_DIVIDER in notebook_content:
        return notebook_content.split(SQL_CELL_DIVIDER)

    return notebook_content.split(PYTHON_CELL_DIVIDER)


def join_python_notebook_cells(cells: List[str]) -> str:
    return PYTHON_CELL_DIVIDER.join(cells)


def remove_blacklisted_sql_cells(cells: List[str]):
    blacklist = [METADATA_HEADER, "create widget", "%run"]

    for cell in cells[:]:
        if any(blacklisted_str in cell for blacklisted_str in blacklist):
            cells.remove(cell)
