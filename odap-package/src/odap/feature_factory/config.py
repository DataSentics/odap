import os
from typing import Optional

from odap.common.databricks import resolve_branch
from odap.common.config import get_config_namespace, ConfigNamespace, Config as ConfigType

from odap.common.exceptions import ConfigAttributeMissingException
from odap.common.utils import concat_catalog_db_table


def get_feature_factory_config() -> ConfigType:
    return get_config_namespace(ConfigNamespace.FEATURE_FACTORY)


def get_param(key: str):
    config = get_feature_factory_config()
    param = config.get(key)

    if not param:
        raise ConfigAttributeMissingException(f"'{key}' not defined in config.yaml")
    return param


def resolve_dev_database_name(database: str):
    if os.environ.get("BRANCH_PREFIX") == "on":
        branch = resolve_branch()
        database = f"{branch}_{database}"

    return database


class Config:
    def __init__(self, config: ConfigType):
        self.__config = config

    @classmethod
    def load_feature_factory_config(cls):
        return cls(get_feature_factory_config())

    def __get_entities(self):
        entities = self.__config.get("entities")

        if not entities:
            raise ConfigAttributeMissingException("entities not defined in config.yaml")
        return entities

    def __get_metadata(self):
        metadata = self.__config.get("metadata")

        if not metadata:
            raise ConfigAttributeMissingException("metadata not defined in config.yaml")

        return metadata

    def __get_features(self):
        features = self.__config.get("features")

        if not features:
            raise ConfigAttributeMissingException("features not defined in config.yaml")

        return features

    def __get_database(self) -> str:
        entity_name = self.get_entity()

        return self.get_database_for_entity(entity_name)

    def __preview_catalog_enabled(self) -> bool:
        return self.__config.get("preview_catalog") is not None

    def get_entity_by_name(self, entity_name: str):
        entity = self.__get_entities().get(entity_name)

        if not entity:
            raise ConfigAttributeMissingException(f"entity '{entity_name}' not defined in config.yaml")
        return entity

    def get_entity(self) -> str:
        entities = self.__get_entities()

        return next(iter(entities))

    def get_entity_primary_key(self) -> str:
        entities = self.__config.get("entities")

        if not entities:
            raise ConfigAttributeMissingException("entities not defined in config.yaml")

        primary_entity = next(iter(entities))

        return entities[primary_entity]["id_column"].lower()

    def get_entity_primary_key_by_name(self, entity_name: str) -> str:
        entity = self.get_entity_by_name(entity_name)

        return entity["id_column"].lower()

    def get_database_for_entity(self, entity_name: str) -> str:
        features_database = self.__config.get("database")

        if not features_database:
            raise ConfigAttributeMissingException("features.database not defined in config.yaml")

        database = features_database.format(entity=entity_name)

        return resolve_dev_database_name(database)

    def get_catalog(self) -> str:
        catalog = self.__config.get("catalog")

        if not catalog:
            raise ConfigAttributeMissingException("features.catalog not defined in config.yaml")

        return catalog

    def get_features_table(self, table_name: str) -> str:
        database = self.__get_database()

        catalog = self.get_catalog() if self.__preview_catalog_enabled() else "hive_metastore"
        return concat_catalog_db_table(catalog, database, table_name)

    def get_features_table_dir_path(self) -> Optional[str]:
        features_table_path = self.__get_features().get("dir_path")

        return features_table_path.format(entity=self.get_entity()) if features_table_path else None

    def get_features_table_path(self, table_name: str) -> Optional[str]:
        dir_path = self.get_features_table_dir_path()
        return f"{dir_path}/{table_name}" if dir_path else None

    def get_latest_features_table_for_entity(self, entity_name: str) -> str:
        table_name = self.__get_features().get("latest_table")

        if not table_name:
            raise ConfigAttributeMissingException("features.latest_table not defined in config.yaml")

        table_name = table_name.format(entity=entity_name)
        catalog = self.get_catalog()
        database = self.get_database_for_entity(entity_name)
        return concat_catalog_db_table(catalog, database, table_name)

    def get_latest_features_table(self) -> str:
        entity_name = self.get_entity()

        return self.get_latest_features_table_for_entity(entity_name)

    def get_latest_features_table_path(self) -> Optional[str]:
        return self.get_features_table_path("latest")

    def get_metadata_table_for_entity(self, entity_name: str) -> str:
        metadata_table = self.__get_metadata().get("table")

        if not metadata_table:
            raise ConfigAttributeMissingException("metadata.table not defined in config.yaml")

        metadata_table = metadata_table.format(entity=entity_name)
        catalog = self.get_catalog()
        database = self.get_database_for_entity(entity_name)
        return concat_catalog_db_table(catalog, database, metadata_table)

    def get_metadata_table(self) -> str:
        entity_name = self.get_entity()

        return self.get_metadata_table_for_entity(entity_name)

    def get_metadata_table_path(self) -> Optional[str]:
        metadata_table_path = self.__get_metadata().get("path")

        return metadata_table_path.format(entity=self.get_entity()) if metadata_table_path else None

    def use_no_target_optimization(self) -> bool:
        return self.__config.get("no_target_optimization") is not None

    def get_feature_sources(self) -> dict:
        feature_sources = self.__config.get("feature_sources")

        if not feature_sources:
            raise ConfigAttributeMissingException("feature_sources not defined in config.yaml")

        return feature_sources

    def get_checkpoint_dir(self):
        checkpoint_dir = self.__config.get("checkpoint_dir")

        if not checkpoint_dir:
            raise ConfigAttributeMissingException("checkpoint_dir not defined in config.yaml")

        return checkpoint_dir

    def get_checkpoint_interval(self):
        checkpoint_interval = self.__config.get("checkpoint_interval")

        if not checkpoint_interval:
            raise ConfigAttributeMissingException("checkpoint_interval not defined in config.yaml")

        return checkpoint_interval


def get_feature_source_dir(feature_source) -> str:
    path = feature_source.get("path")

    if not path:
        raise ConfigAttributeMissingException(f"path not defined in config.yaml for {feature_source}")

    return path


def get_feature_source_prefix(feature_source) -> Optional[str]:
    return feature_source.get("prefix")
