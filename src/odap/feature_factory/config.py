from typing import Dict, Any

from odap.feature_factory.exceptions import (
    EntityNotDefinedException,
    FeaturesTableNotDefinedException,
    FeaturesTablePathNotDefinedException,
)


def get_entity_primary_key(config: Dict[str, Any]) -> str:
    entities = config.get("entities")

    if not entities:
        raise EntityNotDefinedException("entities not defined in config.yaml")

    primary_entity = next(iter(entities))

    return entities[primary_entity]["id_column"]


def get_features_table(config: Dict[str, Any]) -> str:
    features_table = config.get("table")

    if not features_table:
        raise FeaturesTableNotDefinedException("Features table not defined in config.yaml")

    return features_table


def get_features_table_path(config: Dict[str, Any]) -> str:
    features_table_path = config.get("path")

    if not features_table_path:
        raise FeaturesTablePathNotDefinedException("Features path not defined in config.yaml")

    return features_table_path
