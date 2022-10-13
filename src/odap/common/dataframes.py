from pyspark.sql import DataFrame, SparkSession
from databricks_cli.workspace.api import WorkspaceApi
from odap.common.databricks_context import resolve_dbutils
from odap.common.exceptions import InvalidNoteboookException
from odap.common.utils import get_notebook_content


def get_python_dataframe(notebook_content: str) -> DataFrame:
    globals()["spark"] = SparkSession.getActiveSession()
    globals()["dbutils"] = resolve_dbutils()

    exec(notebook_content, globals())  # pylint: disable=W0122
    return eval("df_final")  # pylint: disable=W0123


def get_sql_dataframe(notebook_content: str) -> DataFrame:
    spark = SparkSession.getActiveSession()

    sql_commands = notebook_content.split("-- COMMAND ----------")
    if "create widget" in sql_commands[0]:
        sql_commands = sql_commands[1:]

    df_command = sql_commands.pop()

    for sql_command in sql_commands:
        spark.sql(sql_command)

    return spark.sql(df_command)


def create_dataframe_from_notebook(notebook_path: str, workspace_api: WorkspaceApi) -> DataFrame:
    notebook_content = get_notebook_content(notebook_path, workspace_api)

    try:
        df = get_python_dataframe(notebook_content)
    except SyntaxError:
        df = get_sql_dataframe(notebook_content)

    if not df:
        raise InvalidNoteboookException(f"Notebook at '{notebook_path}' could not be loaded")

    return df
