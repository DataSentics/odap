import pytest

import pyspark.sql.types as t
from odap.feature_factory.exceptions import MetadataParsingException, MissingMetadataException
from odap.feature_factory.metadata import (
    check_metadata,
    extract_raw_metadata_from_cells,
    resolve_metadata,
)
from odap.feature_factory.metadata_schema import get_metadata_schema


class DataFrameMock:
    def __init__(self, data, columns, schema) -> None:
        self.columns = columns
        self.data = data
        self.schema = schema


BASE_PATH = "full/"
RELATIVE_PATH = "path/to/feature"
FEATURE_PATH = BASE_PATH + RELATIVE_PATH


def test_check_metadata():
    with pytest.raises(MetadataParsingException):
        check_metadata({"unknown_column": "value"}, FEATURE_PATH)

    correct_metadata = {field: "val" for field in get_metadata_schema().fieldNames()}
    check_metadata(correct_metadata, FEATURE_PATH)


def test_missing_metadata_header():
    cells = ["cell1", "# NoMetadataHere", "cell2"]
    with pytest.raises(MissingMetadataException):
        extract_raw_metadata_from_cells(cells, FEATURE_PATH)


def generate_variable_metadata(column: str):
    product, feature, time_window = column.split("_")

    time_map = {"14d": "14 days", "30d": "30 days", "90d": "90 days"}

    description = f"{product} {feature} in {time_map[time_window]}"
    feature_template = f"{{product}}_{feature}_{{time_window}}"
    description_template = f"{{product}} {feature} in {{time_window}}"

    return [column, description, feature_template, description_template]


def test_metadata_integration(mocker):
    metadata_cell = """
# MAGIC %python
# MAGIC metadata = {
# MAGIC     "category": "general",
# MAGIC     "features": {
# MAGIC         "{product}_feature_{time_window}": {
# MAGIC             "description": "{product} feature in {time_window}",
# MAGIC             "dtype": "double",
# MAGIC             "tags": ["tag1", "tag2", "tag3"],
# MAGIC         },
# MAGIC         "{product}_feature2_{time_window}": {
# MAGIC             "description": "{product} feature2 in {time_window}",
# MAGIC             "dtype": "double",
# MAGIC             "tags": ["tag1", "tag2", "tag3"],
# MAGIC         },
# MAGIC     },
# MAGIC }
"""
    mocker.patch("odap.common.utils.get_repository_root_api_path", return_value=BASE_PATH)

    columns = [
        "product1_feature_14d",
        "product2_feature_14d",
        "product1_feature_30d",
        "product2_feature_30d",
        "product3_feature2_30d",
        "product4_feature2_30d",
        "product3_feature2_90d",
        "product4_feature2_90d",
    ]

    df_schema = t.StructType([t.StructField(column, t.DoubleType()) for column in columns])

    df = DataFrameMock([0, 0, 0, 0, 0, 0], columns, df_schema)

    raw_metadata = extract_raw_metadata_from_cells(["", metadata_cell, ""], FEATURE_PATH)

    variable_metadata = [generate_variable_metadata(column) for column in columns]

    expected_metadata = [
        {
            "feature": feature,
            "extra": {"product": feature.split("_")[0], "time_window": feature.split("_")[-1]},
            "description": description,
            "feature_template": f_template,
            "description_template": d_template,
            "dtype": "double",
            "category": "general",
            "tags": ["tag1", "tag2", "tag3"],
            "notebook_name": "feature",
            "notebook_absolute_path": FEATURE_PATH,
            "notebook_relative_path": RELATIVE_PATH,
            "variable_type": "numerical",
        }
        for feature, description, f_template, d_template in variable_metadata
    ]

    assert resolve_metadata(raw_metadata, FEATURE_PATH, df) == expected_metadata