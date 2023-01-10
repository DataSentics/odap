from typing import List
import re
from base64 import b64decode
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.workspace.api import WorkspaceFileInfo


from odap.feature_factory import const
from odap.common.exceptions import InvalidNotebookLanguageException, NotebookException
from odap.common.utils import string_contains_any_pattern

SQL_CELL_DIVIDER = "-- COMMAND ----------"
PYTHON_CELL_DIVIDER = "# COMMAND ----------"

SQL_MAGIC_PREFIX = "-- MAGIC "
PYTHON_MAGIC_PREFIX = "# MAGIC "

COMPILED_SQL_COMMENT_REGEX = re.compile("^--.*$", re.MULTILINE)


def get_notebook_cells(notebook_info: WorkspaceFileInfo, workspace_api: WorkspaceApi) -> List[str]:
    output = workspace_api.client.export_workspace(notebook_info.path, format="SOURCE")
    content = output["content"]
    decoded_content = b64decode(content).decode("utf-8")

    return split_notebok_to_cells(decoded_content, notebook_info)


def get_notebook_info(notebook_path: str, workspace_api: WorkspaceApi) -> WorkspaceFileInfo:
    return workspace_api.get_status(notebook_path)


def split_notebok_to_cells(notebook_content: str, notebook_info: WorkspaceFileInfo) -> List[str]:
    if notebook_info.language == "SQL":
        return notebook_content.split(SQL_CELL_DIVIDER)

    if notebook_info.language == "PYTHON":
        return notebook_content.split(PYTHON_CELL_DIVIDER)

    raise InvalidNotebookLanguageException(
        f"Notebook {notebook_info.path} language {notebook_info.language} is not supported"
    )


def join_python_notebook_cells(cells: List[str]) -> str:
    return PYTHON_CELL_DIVIDER.join(cells)


def remove_blacklisted_cells(cells: List[str]):
    blacklist = [const.METADATA_HEADER_REGEX, const.DQ_CHECKS_HEADER_REGEX, "create widget", "%run"]

    for cell in cells[:]:
        if string_contains_any_pattern(string=cell, patterns=blacklist):
            cells.remove(cell)


def remove_magic_prefixes(content: str) -> str:
    if SQL_MAGIC_PREFIX in content:
        return content.replace(SQL_MAGIC_PREFIX, "")

    return content.replace(PYTHON_MAGIC_PREFIX, "")


def sql_cell_is_runable(content: str) -> bool:
    return COMPILED_SQL_COMMENT_REGEX.sub("", content).strip() != ""


def eval_cell(cell: str, variable_name: str, feature_path: str):
    cell = remove_magic_prefixes(cell)

    exec(cell)  # pylint: disable=W0122

    try:
        return eval(variable_name)  # pylint: disable=W0123
    except NameError as e:
        raise NotebookException(f"{variable_name} not provided.", path=feature_path) from e


def eval_cell_with_header(cells: List[str], feature_path: str, header_regex: str, variable_name: str):
    for current_cell in cells:
        matched_header = re.search(header_regex, current_cell)

        if matched_header:
            start_idx = matched_header.start()
            return eval_cell(current_cell[start_idx:], variable_name, feature_path)

    return None
