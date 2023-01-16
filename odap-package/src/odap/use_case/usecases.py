from typing import List
from odap.segment_factory.config import get_use_cases
from odap.use_case.config import get_use_case_config
from odap.use_case.status import get_status
from odap.use_case.functions import get_export_data, get_unique_segments, get_unique_attributes


def generate_use_cases() -> List[dict]:
    table = []
    use_cases = get_use_cases()
    for use_case in use_cases:
        use_case_config = get_use_case_config(use_case)
        table.append(
            {
                "name": use_case,
                "description": use_case_config["description"] if "description" in use_case_config else "",
                "owner": use_case_config["owner"] if "owner" in use_case_config else "",
                "kpi": use_case_config["kpi"] if "kpi" in use_case_config else [],
                "status": get_status(use_case_config),
                "destinations": list(set(get_export_data(use_case_config, "destination"))),
                "exports": list(use_case_config["exports"].keys()) if "exports" in use_case_config else [],
                "segments": get_unique_segments(use_case_config),
                "model": None,
                "attributes": get_unique_attributes(set(get_export_data(use_case_config, "destination"))),
                "sdms": [],
                "data_sources": [],
            }
        )
    return table
