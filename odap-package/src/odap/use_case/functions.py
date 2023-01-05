from typing import List
from odap.use_case.notebooks import get_sdm_data, get_segment_attributes


def get_export_data(config: dict, value: str):
    data = []
    config = config["exports"] if "exports" in config else {}
    for export in config:
        data.append(config[export][value])
    return data


def get_unique_segments(config: dict) -> List[str]:
    return [list(segment.keys())[0] for segment in get_export_data(config, "segments")]


def get_unique_sdm(config: dict) -> List[str]:
    data = []
    for segment in get_unique_segments(config):
        data += [sdm.split(".")[1] for sdm in get_sdm_data(config["name"], segment)]
    return list(set(data))


def get_unique_attributes(config: dict) -> List[str]:
    data = []
    for segment in get_unique_segments(config):
        data += [attribute.split(".")[1] for attribute in get_segment_attributes(config["name"], segment)]
    return list(set(data))
