import re
from typing import List
from base64 import b64decode
from odap.common.databricks import get_workspace_api
from odap.common.utils import get_repository_root_api_path


def get_segment_data(segment: str) -> str:
    workspace_api = get_workspace_api()
    output = workspace_api.client.export_workspace(
        f"{get_repository_root_api_path()}/segments/{segment}", format="SOURCE"
    )
    content = output["content"]
    return b64decode(content).decode("utf-8").split("COMMAND ----------")[1]


def get_segment_attributes(segment: str) -> List[str]:
    return (
        re.match(
            "SELECT (.*)FROM",
            get_segment_data(segment).replace("\n", ""),
        )
        .group(1)
        .replace(" ", "")
        .split(",")
    )


def get_sdm_data(segment: str) -> List[str]:
    data = []
    for sql in get_segment_data(segment).split("\n"):
        if " AS " in sql:
            data.append(sql.split(" AS ")[0].replace(" ", ""))
    return [*set(data)]
