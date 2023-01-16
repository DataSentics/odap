from base64 import b64decode
from odap.common.databricks import get_workspace_api
from odap.common.utils import get_project_root_fs_path, get_api_path


def get_segment_data(use_case: str, segment: str) -> str:
    workspace_api = get_workspace_api()
    output = workspace_api.client.export_workspace(
        f"{get_api_path(get_project_root_fs_path())}/use_cases/{use_case}/segments/{segment}", format="SOURCE"
    )
    content = output["content"]
    return b64decode(content).decode("utf-8").split("COMMAND ----------")[1]
