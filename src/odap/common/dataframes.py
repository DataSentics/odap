from typing import Any, Dict, List, Union
from pyspark.sql import DataFrame, SparkSession
from databricks_cli.workspace.api import WorkspaceFileInfo

from odap.common.databricks import resolve_dbutils
from odap.common.exceptions import NotebookException, InvalidNotebookLanguageException
from odap.common.notebook import remove_blacklisted_cells, join_python_notebook_cells

PYTHON_DF_NAME = "df_final"


def get_python_dataframe(notebook_cells: List[str], notebook_path: str) -> DataFrame:
    globals()["spark"] = SparkSession.getActiveSession()
    globals()["dbutils"] = resolve_dbutils()

    notebook_content = join_python_notebook_cells(notebook_cells)
    exec(notebook_content, globals())  # pylint: disable=W0122

    try:
        return eval(PYTHON_DF_NAME)  # pylint: disable=W0123
    except NameError as e:
        raise NotebookException(f"{PYTHON_DF_NAME} missing", path=notebook_path) from e


def get_sql_dataframe(notebook_cells: List[str]) -> DataFrame:
    spark = SparkSession.getActiveSession()

    df_command = notebook_cells.pop()

    for cell in notebook_cells:
        spark.sql(cell)

    return spark.sql(df_command)


# pylint: disable=too-many-statements
def create_dataframe_from_notebook_cells(notebook: WorkspaceFileInfo, notebook_cells: List[str]) -> DataFrame:
    remove_blacklisted_cells(notebook_cells)

    if notebook.language == "PYTHON":
        df = get_python_dataframe(notebook_cells, notebook.path)

    elif notebook.language == "SQL":
        df = get_sql_dataframe(notebook_cells)

    else:
        raise InvalidNotebookLanguageException(f"Notebook language {notebook.language} is not supported")

    if not df:
        raise NotebookException("Notebook could not be loaded", path=notebook.path)

    df_with_lower_columns = df.toDF(*[column.lower() for column in df.columns])

    return df_with_lower_columns


def create_dataframe(data: Union[List[Dict[str, Any]], List[List[Any]]], schema) -> DataFrame:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    return spark.createDataFrame(data, schema)  # type: ignore
