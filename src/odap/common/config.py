import os
import enum
import yaml
from typing import Dict, Any
from odap.common.exceptions import ConfigAttributMissingException


CONFIG_NAME = "config.yaml"


class ConfigNamespace(enum.Enum):
    FEATURE_FACTORY = "featurefactory"
    SEGMENT_FACTORY = "segmentfactory"


def get_config_parameters() -> Dict[str, Any]:
    base_path = os.getcwd()
    config_path = os.path.join(base_path, CONFIG_NAME)

    with open(config_path, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)

    parameters = config.get("parameters", None)

    if not parameters:
        raise ConfigAttributMissingException("'parameters' not defined in config.yaml")
    return parameters


def get_config_namespace(namespace: ConfigNamespace) -> Dict[str, Any]:
    parameters = get_config_parameters()

    config = parameters.get(namespace.value, None)

    if not config:
        raise ConfigAttributMissingException(f"'{namespace.value}' not defined in config.yaml")

    return config
