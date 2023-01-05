from odap.common.config import get_config_on_rel_path, CONFIG_NAME
from odap.segment_factory.config import USE_CASES_FOLDER


def get_use_config(use_case: str) -> dict:
    try:
        return get_config_on_rel_path(USE_CASES_FOLDER, use_case, CONFIG_NAME)
    except FileNotFoundError:
        return {
            "name": use_case,
            "description": "",
            "owner": "",
            "kpi": [],
            "status": "concept",
            "destinations": "",
        }
