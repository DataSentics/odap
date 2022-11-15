from typing import Dict, List
from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.window import Window
from odap.common.dataframes import create_dataframe

from odap.common.logger import logger
from odap.common.config import TIMESTAMP_COLUMN

from odap.feature_factory import const
from odap.feature_factory.config import get_features_table_by_entity_name, get_metadata_table_by_entity_name
from odap.feature_factory.dq_checks import execute_soda_checks_from_feature_notebooks
from odap.feature_factory.feature_notebook import FeatureNotebooks
from odap.feature_factory.metadata_schema import get_metadata_schema


def join_dataframes(dataframes: List[DataFrame], join_columns: List[str]) -> DataFrame:
    dataframes = [df.na.drop(how="any", subset=join_columns) for df in dataframes]
    window = Window.partitionBy(*join_columns).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    union_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)
    columns = [col for col in union_df.columns if col not in join_columns]

    logger.info(f"Joining {len(dataframes)} dataframes...")
    joined_df = (
        union_df.select(
            *join_columns,
            *[f.first(column, ignorenulls=True).over(window).alias(column) for column in columns],
        )
        .groupBy(join_columns)
        .agg(*[f.first(column).alias(column) for column in columns])
    )
    logger.info("Join successful.")

    return joined_df


def get_latest_features(entity_name: str, feature_factory_config: Dict) -> DataFrame:
    spark = SparkSession.getActiveSession()
    features_table = get_features_table_by_entity_name(entity_name, feature_factory_config)
    metadata_table = get_metadata_table_by_entity_name(entity_name, feature_factory_config)

    last_compute_date = spark.read.table(metadata_table).select(f.max(const.LAST_COMPUTE_DATE)).collect()[0][0]

    return spark.read.table(features_table).filter(f.col(TIMESTAMP_COLUMN) == last_compute_date)


def create_metadata_df(feature_notebooks: FeatureNotebooks):
    features_metadata = []
    for notebook in feature_notebooks:
        features_metadata.extend(notebook.metadata)

    return create_dataframe(features_metadata, get_metadata_schema())


def create_features_df(feature_notebooks: FeatureNotebooks, entity_primary_key: str):
    joined_df = join_dataframes(
        dataframes=[notebook.df for notebook in feature_notebooks], join_columns=[entity_primary_key, TIMESTAMP_COLUMN]
    )

    execute_soda_checks_from_feature_notebooks(df=joined_df, feature_notebooks=feature_notebooks)

    return joined_df
