from typing import Dict, Any
from odap.common.config import ConfigNamespace

from odap.common.exceptions import ConfigAttributeMissingException, InvalidConfigAttributException


def get_segment_table(segment: str, config: Dict[str, Any]) -> str:
    segment_table = config.get("table", None)

    if not segment_table:
        raise ConfigAttributeMissingException(
            f"'{ConfigNamespace.SEGMENT_FACTORY}.segments.table' not defined in config.yaml"
        )

    if not "{segment}" in segment_table:
        raise InvalidConfigAttributException(
            f"Configuration attribute '{ConfigNamespace.SEGMENT_FACTORY}.segments.table' is in the wrong format'"
        )

    return str(segment_table).replace("{segment}", segment)


def get_segment_table_path(segment: str, config: Dict[str, Any]) -> str:
    segment_path = config.get("path", None)

    if not segment_path:
        raise ConfigAttributeMissingException(
            f"'{ConfigNamespace.SEGMENT_FACTORY}.segments.path' not defined in config.yaml"
        )

    if not "{segment}" in segment_path:
        raise InvalidConfigAttributException(
            f"Configuration attribute '{ConfigNamespace.SEGMENT_FACTORY}.segments.path' is in the wrong format'"
        )

    return str(segment_path).replace("{segment}", segment)


def get_segments_dict(config: Dict[str, Any]) -> Dict[str, str]:
    segments_dict = config.get("segments", None)

    if not segments_dict:
        raise ConfigAttributeMissingException(
            f"'{ConfigNamespace.SEGMENT_FACTORY}.segments.list' not defined in config.yaml"
        )

    return segments_dict
