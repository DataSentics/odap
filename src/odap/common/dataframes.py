from typing import List
from pyspark.sql import DataFrame, SparkSession
from odap.common.databricks_context import resolve_dbutils
from odap.common.exceptions import InvalidNoteboookException
from odap.common.utils import join_python_notebook_cells

PYTHON_DF_NAME = "df_final"

def get_python_dataframe(notebook_cells: List[str]) -> DataFrame:
    globals()["spark"] = SparkSession.getActiveSession()
    globals()["dbutils"] = resolve_dbutils()

    notebook_content = join_python_notebook_cells(notebook_cells)
    exec(notebook_content, globals())  # pylint: disable=W0122
    return eval(PYTHON_DF_NAME)  # pylint: disable=W0123

def remove_create_widget_cells(cells: List[str]):
    for cell in cells[:]:
        if "create widget" in cell:
            cells.remove(cell)

def get_sql_dataframe(notebook_cells: List[str]) -> DataFrame:
    spark = SparkSession.getActiveSession()

    remove_create_widget_cells(notebook_cells)

    df_command = notebook_cells.pop()

    for cell in notebook_cells:
        spark.sql(cell)

    return spark.sql(df_command)


def create_dataframe_from_notebook_cells(notebook_path: str, notebook_cells: List[str]) -> DataFrame:
    try:
        df = get_python_dataframe(notebook_cells)
    except SyntaxError:
        df = get_sql_dataframe(notebook_cells)

    if not df:
        raise InvalidNoteboookException(f"Notebook at '{notebook_path}' could not be loaded")

    return df
