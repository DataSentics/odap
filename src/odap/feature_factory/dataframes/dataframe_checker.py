from pyspark.sql import DataFrame

from odap.common.config import TIMESTAMP_COLUMN
from odap.common.exceptions import NotebookException

from odap.feature_factory import const
from odap.feature_factory.metadata_schema import FeaturesMetadataType


def check_feature_df(df: DataFrame, entity_primary_key: str, metadata: FeaturesMetadataType, feature_path: str):
    metadata_features = [feature_metadata[const.FEATURE] for feature_metadata in metadata]

    required_columns = {entity_primary_key, TIMESTAMP_COLUMN}

    for required_column in required_columns:
        if required_column not in df.columns:
            raise NotebookException(f"Required column {required_column} not present in dataframe!", path=feature_path)

    for column in set(df.columns) - required_columns:
        if column not in metadata_features:
            raise NotebookException(f"Column '{column}' from final_df is missing in metadata!", path=feature_path)
