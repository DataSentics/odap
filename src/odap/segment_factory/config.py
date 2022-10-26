from typing import Dict, Any, List, Tuple
from odap.common.config import ConfigNamespace
from odap.common.exceptions import ConfigAttributeMissingException, InvalidConfigAttributException


SEGMENT_FACTORY = ConfigNamespace.SEGMENT_FACTORY.value
Config = Dict[str, Any]


def get_segment_table(segment: str, config: Config) -> str:
    segment_table = config.get("table", None)

    if not segment_table:
        raise ConfigAttributeMissingException(f"'{ConfigNamespace.SEGMENT_FACTORY}.table' not defined in config.yaml")

    if not "{segment}" in segment_table:
        raise InvalidConfigAttributException(
            f"Configuration attribute '{SEGMENT_FACTORY}.table' is in the wrong format'"
        )

    return str(segment_table).replace("{segment}", segment)


def get_segment_table_path(segment: str, config: Config) -> str:
    segment_path = config.get("path", None)

    if not segment_path:
        raise ConfigAttributeMissingException(f"'{ConfigNamespace.SEGMENT_FACTORY}.path' not defined in config.yaml")

    if not "{segment}" in segment_path:
        raise InvalidConfigAttributException(
            f"Configuration attribute '{SEGMENT_FACTORY}.path' is in the wrong format'"
        )

    return str(segment_path).replace("{segment}", segment)


def get_segments(config: Config) -> Dict[str, Any]:
    segments_dict = config.get("segments", None)

    if not segments_dict:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.segments' not defined in config.yaml")

    return segments_dict


def get_flatten_segments_exports(config) -> List[Tuple[str, str]]:
    segments = get_segments(config)
    return [(segment, export) for segment in segments for export in segments[segment]["exports"]]


def get_segment(segment_name: str, config: Config) -> Dict[str, Any]:
    segments_dict = get_segments(config)
    segment_dict = segments_dict.get(segment_name, None)

    if not segment_dict:
        raise ConfigAttributeMissingException(f"Segment '{segment_name}' is not configured in config.yaml.")

    return segment_dict


def get_segments_exports(segment_name: str, config: Config) -> Dict[str, Any]:
    segment_dict = get_segment(segment_name, config)
    exports = segment_dict.get("exports", None)

    if not exports:
        raise ConfigAttributeMissingException(
            f"Exports of the segment '{segment_name}' are not configured in config.yaml."
        )

    return exports


def get_exports(config: Config) -> Dict[str, Any]:
    exporters_dict = config.get("exports", None)

    if not exporters_dict:
        raise ConfigAttributeMissingException(f"'{SEGMENT_FACTORY}.exports' not defined in config.yaml")

    return exporters_dict


# pylint: disable=too-many-statements
def get_export(export_name: str, config: Config) -> Dict[str, Any]:
    exports_dict = get_exports(config)
    export_dict = exports_dict.get(export_name, None)

    if not export_dict:
        raise ConfigAttributeMissingException(f"The export '{export_name}' is not configured in config.yaml.")

    if "type" not in export_dict:
        raise ConfigAttributeMissingException(f"The export '{export_name}' must contain field 'type'.")

    if "attributes" not in export_dict:
        raise ConfigAttributeMissingException(f"The export '{export_name}' must contain field 'attributes'.")

    if not isinstance(export_dict["attributes"], dict):
        raise ConfigAttributeMissingException(f"'{export_name}.attributes' must be a dict.")

    return export_dict
