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


def get_config_on_rel_path(*rel_path: str) -> Config:
    base_path = get_repository_root_fs_path()
    config_path = os.path.join(base_path, *rel_path)

    with open(config_path, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)

    parameters = config.get("parameters", None)

    if not parameters:
        raise ConfigAttributeMissingException(f"'parameters' not defined in {os.path.join(*rel_path)}")
    return parameters


def get_config_namespace(namespace: ConfigNamespace) -> Config:
    parameters = get_config_on_rel_path(CONFIG_NAME)

    config = parameters.get(namespace.value, None)

    if not config:
        raise ConfigAttributeMissingException(f"'{namespace.value}' not defined in config.yaml")

    return config
