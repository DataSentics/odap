from typing import Any, Dict, List, Optional, Tuple
from functools import reduce
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.window import Window
from databricks_cli.workspace.api import WorkspaceApi

from odap.common.logger import logger
from odap.common.config import TIMESTAMP_COLUMN
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.utils import get_notebook_cells
from odap.feature_factory.exceptions import NotebookException
from odap.feature_factory.features import get_features_paths
from odap.feature_factory.metadata import extract_raw_metadata_from_cells, resolve_metadata
from odap.feature_factory.metadata_schema import FEATURE, FeaturesMetadataType


def check_feature_df(df: DataFrame, entity_primary_key: str, metadata: FeaturesMetadataType, feature_path: str):
    metadata_features = [feature_metadata[FEATURE] for feature_metadata in metadata]

    required_columns = {entity_primary_key, TIMESTAMP_COLUMN}

    for required_column in required_columns:
        if required_column not in df.columns:
            raise NotebookException(f"Required column {required_column} not present in dataframe!", feature_path)

    for column in set(df.columns) - required_columns:
        if column not in metadata_features:
            raise NotebookException(f"Column '{column}' from final_df is missing in metadata!", feature_path)


def create_dataframe_and_metadata(feature_path: str, workspace_api: WorkspaceApi):
    notebook_cells = get_notebook_cells(feature_path, workspace_api)

    raw_metadata = extract_raw_metadata_from_cells(notebook_cells, feature_path)

    feature_df = create_dataframe_from_notebook_cells(feature_path, notebook_cells)

    metadata = resolve_metadata(raw_metadata, feature_path, feature_df)

    return feature_df, metadata


def create_dataframes_and_metadata(
    entity_primary_key: str, features_paths: Optional[List[str]] = None
) -> Tuple[List[DataFrame], List[Dict[str, Any]]]:
    workspace_api = get_workspace_api()

    features_paths = features_paths or get_features_paths(workspace_api)

    dataframes = []
    metadata = []

    for feature_path in features_paths:
        df, feature_metadata = create_dataframe_and_metadata(feature_path, workspace_api)
        check_feature_df(df, entity_primary_key, feature_metadata, feature_path)

        logger.info(f"Feature {feature_path} successfully loaded.")

        dataframes.append(df)
        metadata.extend(feature_metadata)

    return dataframes, metadata


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
