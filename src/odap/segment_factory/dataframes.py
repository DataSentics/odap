from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from odap.feature_factory.config import get_entity_primary_key, get_features_table
from odap.segment_factory.config import get_export, get_segment_table


def create_segment_export_dataframe(
    segment_name: str,
    export_name: str,
    feature_factory_config: Dict,
    segment_factory_config: Dict,
) -> DataFrame:
    spark = SparkSession.getActiveSession()

    features_table_name = get_features_table(feature_factory_config)
    segment_table_name = get_segment_table(segment_name, segment_factory_config)
    export_config = get_export(export_name, segment_factory_config)
    primary_key = get_entity_primary_key(feature_factory_config)

    segment_df = spark.read.table(segment_table_name)
    featurestore_df = spark.read.table(features_table_name)

    export_df = segment_df.join(featurestore_df, primary_key).select(
        segment_df["*"],
        *[featurestore_df[col] for col in featurestore_df.columns if col in export_config["attributes"]]
    )
    return export_df
