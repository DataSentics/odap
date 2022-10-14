from typing import Any, Dict, List, Tuple
from functools import reduce
from pyspark.sql import DataFrame, functions as f
from pyspark.sql.window import Window
from odap.common.databricks_context import get_workspace_api, resolve_dbutils
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.utils import get_notebook_cells
from odap.feature_factory.features import get_features_paths
from odap.feature_factory.metadata import extract_metadata_from_cells


def create_dataframes_and_metadata() -> Tuple[List[DataFrame], List[Dict[str, Any]]]:
    dbutils = resolve_dbutils()
    workspace_api = get_workspace_api(dbutils)

    features_paths = get_features_paths(workspace_api)

    dataframes = []
    metadata = []

    for feature_path in features_paths:
        notebook_cells = get_notebook_cells(feature_path, workspace_api)

        metadata.extend(extract_metadata_from_cells(notebook_cells, feature_path))

        feature_dataframe = create_dataframe_from_notebook_cells(feature_path, notebook_cells)
        dataframes.append(feature_dataframe)

    return dataframes, metadata


def join_dataframes(dataframes: List[DataFrame], join_columns: List[str]) -> DataFrame:
    dataframes = [df.na.drop(how="any", subset=join_columns) for df in dataframes]
    window = Window.partitionBy(*join_columns).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    union_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)
    columns = [col for col in union_df.columns if col not in join_columns]

    return (
        union_df.select(
            *join_columns,
            *[f.first(column, ignorenulls=True).over(window).alias(column) for column in columns],
        )
        .groupBy(join_columns)
        .agg(*[f.first(column).alias(column) for column in columns])
    )
