import os
from typing import Optional

from odap.common.databricks import resolve_branch
from odap.common.config import Config

from odap.common.exceptions import ConfigAttributeMissingException
from odap.common.utils import concat_catalog_db_table


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

    return entities[primary_entity]["id_column"].lower()


def get_entity_primary_key_by_name(entity_name: str, config: Config) -> str:
    entity = get_entity_by_name(entity_name, config)

    return entity["id_column"].lower()


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


def resolve_dev_database_name(database: str):
    if os.environ.get("BRANCH_PREFIX") == "on":
        branch = resolve_branch()
        database = f"{branch}_{database}"

    return database


def get_database_for_entity(entity_name: str, config: Config) -> str:
    features_database = config.get("database")

    if not features_database:
        raise ConfigAttributeMissingException("features.database not defined in config.yaml")

    database = features_database.format(entity=entity_name)

    return resolve_dev_database_name(database)


def get_catalog(config: Config) -> str:
    catalog = config.get("catalog")

    if not catalog:
        raise ConfigAttributeMissingException("features.catalog not defined in config.yaml")

    return catalog


def get_database(config: Config) -> str:
    entity_name = get_entity(config)

    return get_database_for_entity(entity_name, config)


def preview_catalog_enabled(config: Config) -> bool:
    return config.get("preview_catalog") is not None


def get_features_table(table_name: str, config: Config) -> str:
    database = get_database(config)

    catalog = get_catalog(config) if preview_catalog_enabled(config) else "hive_metastore"
    return concat_catalog_db_table(catalog, database, table_name)


def get_features_table_path(table_name: str, config: Config) -> Optional[str]:
    dir_path = get_features_table_dir_path(config)
    return f"{dir_path}/{table_name}" if dir_path else None


def get_latest_features_table_for_entity(entity_name: str, config: Config) -> str:
    table_name = get_features(config).get("latest_table")

    if not table_name:
        raise ConfigAttributeMissingException("features.latest_table not defined in config.yaml")

    table_name = table_name.format(entity=entity_name)
    catalog = get_catalog(config)
    database = get_database_for_entity(entity_name, config)
    return concat_catalog_db_table(catalog, database, table_name)


def get_latest_features_table(config: Config) -> str:
    entity_name = get_entity(config)

    return get_latest_features_table_for_entity(entity_name, config)


def get_features_table_dir_path(config: Config) -> Optional[str]:
    features_table_path = get_features(config).get("dir_path")

    return features_table_path.format(entity=get_entity(config)) if features_table_path else None


def get_latest_features_table_path(config: Config) -> Optional[str]:
    return get_features_table_path("latest", config)


def get_metadata_table_for_entity(entity_name: str, config: Config) -> str:
    metadata_table = get_metadata(config).get("table")

    if not metadata_table:
        raise ConfigAttributeMissingException("metadata.table not defined in config.yaml")

    metadata_table = metadata_table.format(entity=entity_name)
    catalog = get_catalog(config)
    database = get_database_for_entity(entity_name, config)
    return concat_catalog_db_table(catalog, database, metadata_table)


def get_metadata_table(config: Config) -> str:
    entity_name = get_entity(config)

    return get_metadata_table_for_entity(entity_name, config)


def get_metadata_table_path(config: Config) -> Optional[str]:
    metadata_table_path = get_metadata(config).get("path")

    return metadata_table_path.format(entity=get_entity(config)) if metadata_table_path else None


def get_ids(config: Config):
    features = config.get("ids")

    if not features:
        raise ConfigAttributeMissingException("ids not defined in config.yaml")

    return features


def get_ids_table(config: Config) -> str:
    ids_table = get_ids(config).get("table")

    if not ids_table:
        raise ConfigAttributeMissingException("ids.table not defined in config.yaml")

    entity_name = get_entity(config)

    return ids_table.format(entity=entity_name)


def use_no_target_optimization(config: Config) -> bool:
    return config.get("no_target_optimization") is not None
