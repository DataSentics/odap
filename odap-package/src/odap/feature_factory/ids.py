from datetime import datetime

from pyspark.sql import functions as f, SparkSession

from odap.common.config import Config, TIMESTAMP_COLUMN
from odap.common.logger import logger
from odap.feature_factory.config import get_entity_primary_key, get_ids_table


def read_ids_table(config: Config):
    entity_id = get_entity_primary_key(config)
    table_name = get_ids_table(config)

    logger.info(f"Reading IDs from table `{table_name}`")
    return SparkSession.getActiveSession().table(table_name).select(entity_id, f.lit(datetime.now()).alias(TIMESTAMP_COLUMN))
