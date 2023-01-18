from odap.common.config import get_config_namespace, get_config_on_rel_path, CONFIG_NAME, ConfigNamespace
from odap.common.exceptions import ConfigAttributeMissingException
from odap.common.utils import concat_catalog_db_table
from odap.segment_factory.config import USE_CASES_FOLDER, Config


USE_CASE_FACTORY = "usecasefactory"


def get_catalog(config: Config) -> str:
    catalog = config.get("catalog")

    if not catalog:
        raise ConfigAttributeMissingException(f"'{USE_CASE_FACTORY}.catalog' not defined in config.yaml")
    return catalog


def get_database(config: Config) -> str:
    database = config.get("database")

    if not database:
        raise ConfigAttributeMissingException(f"'{USE_CASE_FACTORY}.database' not defined in config.yaml")
    return database


def get_use_case_table():
    config = get_config_namespace(ConfigNamespace.USECASE_FACTORY)
    table = config.get("table")

    if not table:
        raise ConfigAttributeMissingException("usecasefactory.table not defined in config.yaml")

    catalog = get_catalog(config)
    database = get_database(config)
    return concat_catalog_db_table(catalog, database, table)


def get_use_case_config(use_case: str) -> dict:
    try:
        config = get_config_on_rel_path(USE_CASES_FOLDER, use_case, CONFIG_NAME)
        config["name"] = use_case
        return config
    except FileNotFoundError:
        return {
            "name": use_case,
            "description": "",
            "owner": "",
            "kpi": [],
            "status": "concept",
            "destinations": "",
        }
