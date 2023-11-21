from typing import List, Iterable

from odap.common.utils import get_project_root_fs_path
from odap.common.config import get_config_on_rel_path, get_config_file_name
from odap.segment_factory.config import USE_CASES_FOLDER


def get_export_data(config: dict, value: str):
    data = []
    config = config["exports"] if "exports" in config else {}
    for export in config:
        data.append(config[export][value])
    return data


def get_unique_segments(config: dict) -> List[str]:
    return [list(segment.keys())[0] for segment in get_export_data(config, "segments")]


def get_unique_attributes(destinations: Iterable[dict]) -> dict:
    config = get_config_on_rel_path(USE_CASES_FOLDER, get_project_root_fs_path(), get_config_file_name())[
        "segmentfactory"
    ]["destinations"]

    result = {}
    for destination in destinations:
        attributes = config[destination]["attributes"]
        for attribute in attributes:
            result[attribute] = (
                list(set(result[attribute] + attributes[attribute])) if attribute in result else attributes[attribute]
            )

    return result
