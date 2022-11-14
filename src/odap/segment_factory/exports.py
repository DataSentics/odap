from typing import Any, Dict
from pyspark.sql import DataFrame
from odap.feature_factory.config import get_entity_by_name
from odap.feature_factory.dataframes.dataframe_creator import get_latest_features
from odap.segment_factory.config import get_destination, get_export
from odap.segment_factory.exporters import resolve_exporter
from odap.segment_factory.logs import write_export_log
from odap.segment_factory.schemas import SEGMENT
from odap.segment_factory.segments import create_segments_union_df
from odap.common.logger import logger


def join_segment_with_entities(segment_df: DataFrame, destination_config: Any, feature_factory_config: Dict):
    for entity_name in destination_config.get("attributes").keys():
        id_column = get_entity_by_name(entity_name, feature_factory_config).get("id_column")

        latest_features_df = get_latest_features(entity_name, feature_factory_config)

        if (id_column not in segment_df.columns) or (id_column not in latest_features_df.columns):
            raise Exception(f"'{id_column}' column is missing in the segment or entity dataframe")
        segment_df = segment_df.join(latest_features_df, id_column, "inner")

    return segment_df


def select_attributes(df: DataFrame, destination_config: Any):
    select_columns = [
        attribute for attributes in destination_config.get("attributes").values() for attribute in attributes
    ]
    return df.select(SEGMENT, *select_columns)


# pylint: disable=too-many-statements
def run_export(export_name: str, feature_factory_config: Dict, segment_factory_config: Dict):
    logger.info(f"Running export '{export_name}'...")

    export_config = get_export(export_name, segment_factory_config)
    destination_config = get_destination(export_config["destination"], segment_factory_config)

    united_segments_df = create_segments_union_df(export_config["segments"])
    joined_segment_featurestores_df = join_segment_with_entities(
        united_segments_df, destination_config, feature_factory_config
    )
    final_export_df = select_attributes(joined_segment_featurestores_df, destination_config)

    exporter_fce = resolve_exporter(destination_config["type"])
    exporter_fce(export_name, final_export_df, export_config, destination_config)

    write_export_log(united_segments_df, export_name, segment_factory_config)

    logger.info("Export successful.")
