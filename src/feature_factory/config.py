import os
from typing import Dict, Any
import yaml

from feature_factory.exceptions import (
    EntityNotDefinedException,
    FeaturesNotDefinedException,
    FeaturesTableNotDefinedException,
    FeaturesTablePathNotDefinedException,
)


def get_config() -> Dict[str, Any]:
    base_path = os.getcwd()
    config_path = os.path.join(base_path, "config.yaml")

    with open(config_path, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)

    return config["parameters"]


def get_entity_primary_key(config: Dict[str, Any]) -> str:
    entities = config.get("entities")

    if not entities:
        raise EntityNotDefinedException("entities not defined in config.yaml")

    primary_entity = next(iter(entities))

    return entities[primary_entity]["id_column"]


def get_features(config: Dict[str, Any]) -> Dict[str, str]:
    features = config.get("features")

    if not features:
        raise FeaturesNotDefinedException("Features not defined in config.yaml")

    return features


def get_features_table(config: Dict[str, Any]) -> str:
    features = get_features(config)

    features_table = features.get("table")

    if not features_table:
        raise FeaturesTableNotDefinedException("Features table not defined in config.yaml")

    return features_table


def get_features_table_path(config: Dict[str, Any]) -> str:
    features = get_features(config)

    features_table_path = features.get("path")

    if not features_table_path:
        raise FeaturesTablePathNotDefinedException("Features path not defined in config.yaml")

    return features_table_path
