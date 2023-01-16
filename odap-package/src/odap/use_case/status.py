from odap.common.databricks import get_workspace_api, get_repos_api
from odap.common.utils import get_repository_info


def get_status(use_case_config: dict) -> str:
    if "status" in use_case_config:
        return "concept"

    repository = get_repository_info(workspace_api=get_workspace_api(), repos_api=get_repos_api())

    if repository.get("branch", "") != "master":
        return "dev"

    return "production"
