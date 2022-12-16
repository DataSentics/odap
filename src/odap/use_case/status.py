from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.repos.api import ReposApi
from odap.common.databricks import get_host, get_token, resolve_dbutils
from odap.common.utils import get_repository_root_api_path


def get_branch() -> str:
    dbutils = resolve_dbutils()
    api_client = ApiClient(host=get_host(), token=get_token(dbutils))
    return ReposApi(api_client).list(get_repository_root_api_path(), "")["repos"][0]["branch"]


def get_status(use_case_config: dict) -> str:
    if "status" in use_case_config:
        return "concept"
    if use_case_config["status"] != "master":
        return "dev"
    return "production"
