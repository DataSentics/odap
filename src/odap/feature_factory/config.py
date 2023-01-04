from odap.common.config import Config

from odap.common.exceptions import ConfigAttributeMissingException
from odap.common.widgets import get_widget_value
from odap.feature_factory import const


def get_entities(config: Config) -> Config:
    entities = config.get("entities")

    if not entities:
        raise ConfigAttributeMissingException("entities not defined in config.yaml")
    return entities


def get_entity_by_name(entity_name: str, config: Config):
    entity = get_entities(config).get(entity_name)

    if not entity:
        raise ConfigAttributeMissingException(f"entity '{entity_name}' not defined in config.yaml")
    return entity


def get_entity(config: Config) -> str:
    entities = get_entities(config)

    return next(iter(entities))


def get_entity_primary_key(config: Config) -> str:
    entities = config.get("entities")

    if not entities:
        raise ConfigAttributeMissingException("entities not defined in config.yaml")

    primary_entity = next(iter(entities))

    return entities[primary_entity]["id_column"]


def get_features(config: Config):
    features = config.get("features")

    if not features:
        raise ConfigAttributeMissingException("features not defined in config.yaml")

    return features


def get_metadata(config: Config):
    metadata = config.get("metadata")

    if not metadata:
        raise ConfigAttributeMissingException("metadata not defined in config.yaml")

    return metadata


def get_database_for_entity(entity_name: str, config: Config) -> str:
    features_database = config.get("database")

    if not features_database:
        raise ConfigAttributeMissingException("features.database not defined in config.yaml")

    return features_database.format(entity=entity_name)


def get_database(config: Config) -> str:
    entity_name = get_entity(config)

    return get_database_for_entity(entity_name, config)


def concat_db_table(database: str, table_name: str) -> str:
    return f"{database}.{table_name}"


def get_features_table(table_name: str, config: Config) -> str:
    database = get_database(config)

    return concat_db_table(database, table_name)


def get_latest_features_table_for_entity(entity_name: str, config: Config) -> str:
    table_name = get_features(config).get("latest_table")

    if not table_name:
        raise ConfigAttributeMissingException("features.latest_table not defined in config.yaml")

    table_name = table_name.format(entity=entity_name)
    database = get_database_for_entity(entity_name, config)
    return concat_db_table(database, table_name)


def get_latest_features_table(config: Config) -> str:
    entity_name = get_entity(config)

    return get_latest_features_table_for_entity(entity_name, config)


def get_features_table_dir_path(config: Config) -> str:
    features_table_path = get_features(config).get("dir_path")

    if not features_table_path:
        raise ConfigAttributeMissingException("features.dir_path not defined in config.yaml")

    return features_table_path.format(entity=get_entity(config))


def get_latest_features_table_path(config: Config) -> str:
    return f"{get_features_table_dir_path(config)}/latest"


def get_metadata_table_for_entity(entity_name: str, config: Config) -> str:
    metadata_table = get_metadata(config).get("table")

    if not metadata_table:
        raise ConfigAttributeMissingException("metadata.table not defined in config.yaml")

    metadata_table = metadata_table.format(entity=entity_name)
    database = get_database_for_entity(entity_name, config)
    return concat_db_table(database, metadata_table)


def get_metadata_table(config: Config) -> str:
    entity_name = get_entity(config)

    return get_metadata_table_for_entity(entity_name, config)


def get_metadata_table_path(config: Config) -> str:
    metadata_table_path = get_metadata(config).get("path")

    if not metadata_table_path:
        raise ConfigAttributeMissingException("metadata.path not defined in config.yaml")

    return metadata_table_path.format(entity=get_entity(config))


def is_no_target_mode() -> bool:
    return get_widget_value(const.TARGET_WIDGET).strip() == const.NO_TARGET
