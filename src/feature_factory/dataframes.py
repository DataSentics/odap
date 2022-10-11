from typing import List
from functools import reduce
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession, functions as f
from pyspark.sql.window import Window
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.sdk.api_client import ApiClient

from feature_factory.databricks_context import get_host, get_token, resolve_dbutils
from feature_factory.exceptions import InvalidFeatureException
from feature_factory.features import get_feature_content, get_features_paths


def get_python_dataframe(feature: str) -> DataFrame:
    dbutils = resolve_dbutils()  # pylint: disable=W0641
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641

    exec(feature, globals(), locals())  # pylint: disable=W0122
    return eval("df_final")  # pylint: disable=W0123


def get_sql_dataframe(feature: str) -> DataFrame:
    spark = SparkSession.getActiveSession()

    sql_commands = feature.split("-- COMMAND ----------")
    if "create widget" in sql_commands[0]:
        sql_commands = sql_commands[1:]

    df_command = sql_commands.pop()

    for sql_command in sql_commands:
        spark.sql(sql_command)

    return spark.sql(df_command)


def get_workspace_api(dbutils: DBUtils) -> WorkspaceApi:
    api_client = ApiClient(host=get_host(dbutils), token=get_token(dbutils))
    return WorkspaceApi(api_client)


def create_feature_dataframe(workspace_api: WorkspaceApi, feature_path: str) -> DataFrame:
    feature_content = get_feature_content(workspace_api, feature_path)

    try:
        df = get_python_dataframe(feature_content)
    except SyntaxError:
        df = get_sql_dataframe(feature_content)

    if not df:
        raise InvalidFeatureException(f"Feature at {feature_path} could not be loaded")

    return df


def create_dataframes() -> List[DataFrame]:
    dbutils = resolve_dbutils()
    workspace_api = get_workspace_api(dbutils)

    features_paths = get_features_paths(dbutils, workspace_api)

    dataframes = []

    for feature_path in features_paths:
        feature_dataframe = create_feature_dataframe(workspace_api, feature_path)
        dataframes.append(feature_dataframe)

    return dataframes


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
