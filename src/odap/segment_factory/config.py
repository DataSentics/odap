from typing import Dict, Any, List
from odap.common.config import get_config_on_rel_path, ConfigNamespace, CONFIG_NAME
from odap.common.utils import get_absolute_api_path, list_folders
from odap.common.exceptions import ConfigAttributeMissingException
from odap.common.databricks import get_workspace_api

USE_CASES_FOLDER = "use_cases"
SEGMENTS_FOLDER = "segments"
SEGMENT_FACTORY = ConfigNamespace.SEGMENT_FACTORY.value
Config = Dict[str, Any]


def get_segment_table(config: Config) -> str:
    segment_table = config.get("segment", {}).get("table")

    if not segment_table:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.segment.table' not defined in config.yaml")
    return segment_table


def get_segment_table_path(config: Config) -> str:
    segment_path = config.get("segment", {}).get("path")

    if not segment_path:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.segment.path' not defined in config.yaml")
    return segment_path


def get_log_table(config: Config) -> str:
    log_table = config.get("log", {}).get("table")

    if not log_table:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.log.table' not defined in config.yaml")
    return log_table


def get_log_table_path(config: Config) -> str:
    log_path = config.get("log", {}).get("path")

    if not log_path:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.log_path' not defined in config.yaml")
    return log_path


def get_exports(config: Config) -> Dict[str, Any]:
    exports_dict = config.get("exports", None)

    if not exports_dict:
        raise ConfigAttributeMissingException("'exports' not defined in config.yaml")

    return exports_dict


def get_export(export_name: str, config: Config) -> Dict[str, Any]:
    export_dict = get_exports(config).get(export_name, None)

    if not export_dict:
        raise ConfigAttributeMissingException(f"Export '{export_name}' not defined in config.yaml.")

    return export_dict


def get_destinations(config: Config) -> Dict[str, Any]:
    destinations_dict = config.get("destinations", None)

    if not destinations_dict:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.destinations' not defined in config.yaml")
    return destinations_dict


def get_destination(destination_name: str, config: Config) -> Dict[str, Any]:
    destination_dict = get_destinations(config).get(destination_name)

    if not destination_dict:
        raise ConfigAttributeMissingException(
            f"'{SEGMENT_FACTORY}.destinations.{destination_name}' not defined in config.yaml."
        )

    return destination_dict


def get_use_cases() -> List[str]:
    worskpace_api = get_workspace_api()

    use_cases_path = get_absolute_api_path(USE_CASES_FOLDER)
    use_cases_folders = list_folders(use_cases_path, worskpace_api)

    return [use_case_folder.basename for use_case_folder in use_cases_folders]


def get_use_case_config(use_case_name: str) -> Dict[str, Any]:
    return get_config_on_rel_path(USE_CASES_FOLDER, use_case_name, CONFIG_NAME)
