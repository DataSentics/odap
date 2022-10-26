from typing import Any, Dict
from pyspark.sql import DataFrame, SparkSession
from odap.feature_factory.config import (
    get_entity_by_name,
    get_features_table_by_entity_name,
)


DataFramesMap = Dict[str, DataFrame]


def create_entities_dataframes(
    export_config: Any,
    feature_factory_config: Dict,
) -> DataFramesMap:
    spark = SparkSession.getActiveSession()
    dataframes_map = {}

    for entity_name in export_config.get("attributes").keys():
        table = get_features_table_by_entity_name(entity_name, feature_factory_config)
        dataframes_map[entity_name] = spark.read.table(table)

    return dataframes_map


def join_segment_with_entities(
    segment_dataframe: DataFrame, entities_dataframes: DataFramesMap, feature_factory_config: Dict
):
    for entity_name, entity_dataframe in entities_dataframes.items():
        id_column = get_entity_by_name(entity_name, feature_factory_config).get("id_column")

        if (not id_column in segment_dataframe.columns) or (not id_column in entity_dataframe.columns):
            raise Exception(f"'{id_column}' column is missing in the segment or entity dataframe")

        segment_dataframe = segment_dataframe.join(entity_dataframe, id_column, "inner")

    return segment_dataframe


def select_attributes(dataframe: DataFrame, export_config: Any, entities_dataframes: DataFramesMap):
    select_columns = []

    for entity, attributes in export_config.get("attributes").items():
        for attribute in attributes:
            select_columns.append(entities_dataframes[entity][attribute])

    return dataframe.select(*select_columns)


def create_export_dataframe(
    segment_dataframe: DataFrame, export_config: Dict, feature_factory_config: Dict
) -> DataFrame:
    entities_dataframes = create_entities_dataframes(export_config, feature_factory_config)

    joined_dataframe = join_segment_with_entities(segment_dataframe, entities_dataframes, feature_factory_config)

    selected_dataframe = select_attributes(joined_dataframe, export_config, entities_dataframes)

    return selected_dataframe
