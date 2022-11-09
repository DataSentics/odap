from typing import Dict, Any

from odap.common.exceptions import ConfigAttributeMissingException


def get_entities(config: Dict[str, Any]) -> Dict[str, Any]:
    entities = config.get("entities")

    if not entities:
        raise ConfigAttributeMissingException("entities not defined in config.yaml")
    return entities


def get_entity_by_name(entity_name: str, config: Dict[str, Any]):
    entity = get_entities(config).get(entity_name)

    if not entity:
        raise ConfigAttributeMissingException(f"entity '{entity_name}' not defined in config.yaml")
    return entity


def get_entity(config: Dict[str, Any]) -> str:
    entities = get_entities(config)

    return next(iter(entities))


def get_entity_primary_key(config: Dict[str, Any]) -> str:
    entities = config.get("entities")

    if not entities:
        raise ConfigAttributeMissingException("entities not defined in config.yaml")

    primary_entity = next(iter(entities))

    return entities[primary_entity]["id_column"]


def get_features(config: Dict[str, Any]):
    features = config.get("features")

    if not features:
        raise ConfigAttributeMissingException("features not defined in config.yaml")

    return features


def get_metadata(config: Dict[str, Any]):
    metadata = config.get("metadata")

    if not metadata:
        raise ConfigAttributeMissingException("metadata not defined in config.yaml")

    return metadata


def get_features_table_by_entity_name(entity_name: str, config: Dict[str, Any]) -> str:
    features_table: str = get_features(config).get("table")

    if not features_table:
        raise ConfigAttributeMissingException("features.table not defined in config.yaml")

    return features_table.format(entity=entity_name)


def get_metadata_table_by_entity_name(entity_name: str, config: Dict[str, Any]) -> str:
    metadata_table: str = get_metadata(config).get("table")

    if not metadata_table:
        raise ConfigAttributeMissingException("metadata.table not defined in config.yaml")

    return metadata_table.format(entity=entity_name)


def get_features_table(config: Dict[str, Any]) -> str:
    features_table = get_features(config).get("table")

    if not features_table:
        raise ConfigAttributeMissingException("features.table not defined in config.yaml")

    return features_table.format(entity=get_entity(config))


def get_features_table_path(config: Dict[str, Any]) -> str:
    features_table_path = get_features(config).get("path")

    if not features_table_path:
        raise ConfigAttributeMissingException("features.path not defined in config.yaml")

    return features_table_path.format(entity=get_entity(config))


def get_latest_features_table(config: Dict[str, Any]) -> str:
    return f"{get_features_table(config)}_latest"


def get_latest_features_table_path(config: Dict[str, Any]) -> str:
    return f"{get_features_table_path(config)}.latest"


def get_metadata_table(config: Dict[str, Any]) -> str:
    metadata_table = get_metadata(config).get("table")

    if not metadata_table:
        raise ConfigAttributeMissingException("metadata.table not defined in config.yaml")

    return metadata_table.format(entity=get_entity(config))


def get_metadata_table_path(config: Dict[str, Any]) -> str:
    metadata_table_path = get_metadata(config).get("path")

    if not metadata_table_path:
        raise ConfigAttributeMissingException("metadata.path not defined in config.yaml")

    return metadata_table_path.format(entity=get_entity(config))
