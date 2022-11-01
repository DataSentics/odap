import IPython
from pyspark.dbutils import DBUtils
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.sdk.api_client import ApiClient


def resolve_dbutils() -> DBUtils:
    ipython = IPython.get_ipython()

    if not hasattr(ipython, "user_ns") or "dbutils" not in ipython.user_ns:
        raise Exception("dbutils cannot be resolved")

    return ipython.user_ns["dbutils"]


def get_workspace_api() -> WorkspaceApi:
    dbutils = resolve_dbutils()

    api_client = ApiClient(host=get_host(dbutils), token=get_token(dbutils))
    return WorkspaceApi(api_client)


def get_host(dbutils: DBUtils) -> str:
    return f"https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}"


def get_token(dbutils: DBUtils) -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def get_widget_value(widget_name: str):
    dbutils = resolve_dbutils()

    return dbutils.widgets.get(widget_name)
