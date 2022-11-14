from typing import Dict, Any
import os
import enum
import yaml
from odap.common.utils import get_repository_root_fs_path
from odap.common.exceptions import ConfigAttributeMissingException


CONFIG_NAME = "config.yaml"
TIMESTAMP_COLUMN = "timestamp"
Config = Dict[str, Any]


class ConfigNamespace(enum.Enum):
    FEATURE_FACTORY = "featurefactory"
    SEGMENT_FACTORY = "segmentfactory"


def get_config_parameters() -> Config:
    base_path = get_repository_root_fs_path()
    config_path = os.path.join(base_path, CONFIG_NAME)

    with open(config_path, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)

    parameters = config.get("parameters", None)

    if not parameters:
        raise ConfigAttributeMissingException("'parameters' not defined in config.yaml")
    return parameters


def get_config_namespace(namespace: ConfigNamespace) -> Config:
    parameters = get_config_parameters()

    config = parameters.get(namespace.value, None)

    if not config:
        raise ConfigAttributeMissingException(f"'{namespace.value}' not defined in config.yaml")

    return config
