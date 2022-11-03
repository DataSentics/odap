from typing import Dict
from odap.common.config import TIMESTAMP_COLUMN
from odap.feature_factory.metadata_schema import LAST_COMPUTE_DATE
from odap.segment_factory.config import get_export, get_segment
from odap.segment_factory.exporters import resolve_exporter
from odap.segment_factory.logs import write_export_log
from odap.segment_factory.segments import create_segment_df
from typing import Any, Dict
from pyspark.sql import DataFrame, SparkSession, functions as f
from odap.feature_factory.config import (
    get_entity_by_name,
    get_features_table_by_entity_name,
    get_metadata_table_by_entity_name,
)


def create_leatest_features_df(entity_name: str, feature_factory_config: Dict):
    spark = SparkSession.getActiveSession()
    features_table = get_features_table_by_entity_name(entity_name, feature_factory_config)
    metadata_table = get_metadata_table_by_entity_name(entity_name, feature_factory_config)

    last_compute_df = spark.read.table(metadata_table).select(f.max(LAST_COMPUTE_DATE).alias(LAST_COMPUTE_DATE))

    return (
        spark.read.table(features_table)
        .join(last_compute_df, how="full")
        .filter(f.col(TIMESTAMP_COLUMN) == f.col(LAST_COMPUTE_DATE))
    )


def join_segment_with_entities(segment_df: DataFrame, export_config: Any, feature_factory_config: Dict):
    for entity_name in export_config.get("attributes").keys():
        id_column = get_entity_by_name(entity_name, feature_factory_config).get("id_column")

        leatest_features_df = create_leatest_features_df(entity_name, feature_factory_config)

        if (not id_column in segment_df.columns) or (not id_column in leatest_features_df.columns):
            raise Exception(f"'{id_column}' column is missing in the segment or entity dataframe")
        segment_df = segment_df.join(leatest_features_df, id_column, "inner")

    return segment_df


def select_attributes(df: DataFrame, export_config: Any):
    select_columns = [attribute for attributes in export_config.get("attributes").values() for attribute in attributes]
    return df.select(*select_columns)


def run_export(segment_name: str, export_name: str, feature_factory_config: Dict, segment_factory_config: Dict):
    segment_config = get_segment(segment_name, segment_factory_config)
    export_config = get_export(export_name, segment_factory_config)

    segment_df = create_segment_df(segment_name)
    joined_segment_featurestores_df = join_segment_with_entities(segment_df, export_config, feature_factory_config)
    final_export_df = select_attributes(joined_segment_featurestores_df, export_config)

    exporter_fce = resolve_exporter(export_config["type"])
    exporter_fce(segment_name, final_export_df, segment_config, export_config)

    write_export_log(segment_df, segment_name, export_name, segment_factory_config)
