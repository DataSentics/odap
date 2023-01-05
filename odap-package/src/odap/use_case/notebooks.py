import re
from typing import List
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


def get_segment_attributes(use_case: str, segment: str) -> List[str]:
    segments = re.match(
        "SELECT (.*)FROM",
        get_segment_data(use_case, segment).replace("\n", ""),
    )
    if not segments:
        return []
    return segments.group(1).replace(" ", "").split(",")


def get_sdm_data(use_case: str, segment: str) -> List[str]:
    data = []
    for sql in get_segment_data(use_case, segment).split("\n"):
        if " AS " in sql:
            data.append(sql.split(" AS ")[0].replace(" ", ""))
    return list(set(data))
