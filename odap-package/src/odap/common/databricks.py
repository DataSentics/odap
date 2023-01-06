import IPython
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.repos.api import ReposApi
from databricks_cli.sdk.api_client import ApiClient
from odap.common.utils import get_repository_info
from odap.common.exceptions import UnableToResolveBranchException


def resolve_dbutils() -> DBUtils:
    return resolve_databricks_service("dbutils")


def resolve_display():
    return resolve_databricks_service("display")


def resolve_display_html():
    return resolve_databricks_service("displayHTML")


def resolve_databricks_service(name: str):
    ipython = IPython.get_ipython()

    if not hasattr(ipython, "user_ns") or name not in ipython.user_ns:  # type: ignore
        raise Exception(f"{name} cannot be resolved")

    return ipython.user_ns[name]  # type: ignore


def get_workspace_api() -> WorkspaceApi:
    dbutils = resolve_dbutils()

    api_client = ApiClient(host=get_host(), token=get_token(dbutils))
    return WorkspaceApi(api_client)


def get_repos_api() -> ReposApi:
    dbutils = resolve_dbutils()

    api_client = ApiClient(host=get_host(), token=get_token(dbutils))
    return ReposApi(api_client)


def get_host() -> str:
    spark = SparkSession.getActiveSession()
    return f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"


def get_token(dbutils: DBUtils) -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def resolve_branch() -> str:
    context = get_repository_info(workspace_api=get_workspace_api(), repos_api=get_repos_api())

    branch = context.get("branch")

    if not branch:
        raise UnableToResolveBranchException("Cannot resolve current branch!")

    return branch
