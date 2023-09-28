from typing import Dict, List, Iterable
from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.window import Window
from odap.common.dataframes import create_dataframe

from odap.common.logger import logger
from odap.common.config import TIMESTAMP_COLUMN

from odap.feature_factory import const
from odap.feature_factory.config import (
    get_metadata_table,
)
from odap.feature_factory.dq_checks import execute_soda_checks_from_feature_notebooks
from odap.feature_factory.feature_notebook import FeatureNotebookList
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


def get_all_feature_tables(config: Dict) -> Iterable[str]:
    spark = SparkSession.getActiveSession()
    metadata_table = get_metadata_table(config)
    return {row.table for row in spark.table(metadata_table).select(const.TABLE).collect()}


def create_metadata_df(feature_notebooks: FeatureNotebookList) -> DataFrame:
    features_metadata = []
    for notebook in feature_notebooks:
        features_metadata.extend(notebook.metadata)

    return create_dataframe(features_metadata, get_metadata_schema())


def fill_nulls_in_notebook(notebook: List[Dict], prefix: str) -> Dict:
    fill_dict = {}

    for feature in notebook:
        if feature[const.FILLNA_VALUE_TYPE] == "NoneType":
            continue
        if feature[const.DTYPE].startswith("array"):
            continue
        feature_name = f"{prefix}_{feature[const.FEATURE]}"
        fill_dict[feature_name] = feature[const.FILLNA_VALUE]
    return fill_dict


def fill_array_nulls(df: DataFrame, notebook: List[Dict], prefix: str) -> DataFrame:
    for feature in notebook:
        if feature[const.DTYPE].startswith("array") and feature[const.FILLNA_VALUE] is not None:
            feature_name = f"{prefix}_{feature[const.FEATURE]}"
            df = df.withColumn(
                feature_name,
                f.when(f.col(feature_name).isNull(), f.array(*map(f.lit, feature[const.FILLNA_VALUE]))).otherwise(
                    f.col(feature_name)
                ),
            )
    return df


def fill_nulls(df: DataFrame, feature_notebooks: FeatureNotebookList, prefix: str) -> DataFrame:
    metadata = [notebook.metadata for notebook in feature_notebooks]
    fill_dict = {}

    for notebook in metadata:
        notebook_dict = fill_nulls_in_notebook(notebook, prefix)
        fill_dict.update(notebook_dict)

    for notebook in metadata:
        df = fill_array_nulls(df, notebook, prefix)

    return df.fillna(fill_dict)


def create_features_df(feature_notebooks: FeatureNotebookList, entity_primary_key: str, prefix: str) -> DataFrame:
    joined_df = join_dataframes(
        dataframes=[notebook.df for notebook in feature_notebooks], join_columns=[entity_primary_key, TIMESTAMP_COLUMN]
    )
    if prefix:
        columns = joined_df.columns
        renamed_columns = [
            f"{prefix}_{col}" if col not in [entity_primary_key, TIMESTAMP_COLUMN] else col for col in columns
        ]
        joined_df = joined_df.toDF(*renamed_columns)

    filled_df = fill_nulls(joined_df, feature_notebooks, prefix)

    execute_soda_checks_from_feature_notebooks(df=filled_df, feature_notebooks=feature_notebooks)
    return filled_df
