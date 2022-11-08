from typing import Any, Dict, List, Union
from pyspark.sql import DataFrame, SparkSession
from odap.common.databricks import resolve_dbutils
from odap.common.exceptions import InvalidNoteboookException, InvalidNotebookLanguageException
from odap.common.utils import join_python_notebook_cells
from odap.feature_factory.metadata import METADATA_HEADER

PYTHON_DF_NAME = "df_final"


def get_python_dataframe(notebook_cells: List[str], notebook_path: str) -> DataFrame:
    globals()["spark"] = SparkSession.getActiveSession()
    globals()["dbutils"] = resolve_dbutils()

    notebook_content = join_python_notebook_cells(notebook_cells)
    exec(notebook_content, globals())  # pylint: disable=W0122

    try:
        return eval(PYTHON_DF_NAME)  # pylint: disable=W0123
    except NameError as e:
        raise InvalidNoteboookException(f"{PYTHON_DF_NAME} missing in {notebook_path}") from e


def remove_blacklisted_cells(cells: List[str]):
    blacklist = [METADATA_HEADER, "create widget", "%run"]

    for cell in cells[:]:
        if any(blacklisted_str in cell for blacklisted_str in blacklist):
            cells.remove(cell)


def get_sql_dataframe(notebook_cells: List[str]) -> DataFrame:
    spark = SparkSession.getActiveSession()

    remove_blacklisted_cells(notebook_cells)

    df_command = notebook_cells.pop()

    for cell in notebook_cells:
        spark.sql(cell)

    return spark.sql(df_command)


def create_dataframe_from_notebook_cells(
    notebook_path: str, notebook_language: str, notebook_cells: List[str]
) -> DataFrame:
    if notebook_language == "PYTHON":
        df = get_python_dataframe(notebook_cells, notebook_path)

    elif notebook_language == "SQL":
        df = get_sql_dataframe(notebook_cells)

    else:
        raise InvalidNotebookLanguageException(f"Notebook language {notebook_language} is not supported")

    if not df:
        raise InvalidNoteboookException(f"Notebook at '{notebook_path}' could not be loaded")

    df_with_lower_columns = df.toDF(*[column.lower() for column in df.columns])

    return df_with_lower_columns


def create_dataframe(data: Union[List[Dict[str, Any]], List[List[Any]]], schema) -> DataFrame:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    return spark.createDataFrame(data, schema)  # type: ignore
