import pytest

import pyspark.sql.types as t
from odap.feature_factory.exceptions import MetadataParsingException, MissingMetadataException
from odap.feature_factory.metadata import (
    extract_metadata_string_from_cells,
    get_feature_name,
    get_metadata_dict_from_line,
    parse_metadata,
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


def test_metadata_regex_invalid():
    invalid_lines = [
        " - description: `value`",
        "- description: `value`",
        "  -description: `value`",
        "  - description:`value`",
        "  - description: `value",
        "  - description: value`",
        "  - Description: `value`",
        "  - unknown_column: `value`",
    ]

    for line in invalid_lines:
        with pytest.raises(MetadataParsingException):
            get_metadata_dict_from_line(line, FEATURE_PATH)


def test_metadata_regex_valid():
    valid_lines = [f"  - {field}: `value1`" for field in get_metadata_schema().fieldNames()]

    for line in valid_lines:
        get_metadata_dict_from_line(line, FEATURE_PATH)


def test_feature_regex_invalid():
    invalid_lines = [
        "- `feature_with_invalid_characters#@$`",
        " - `feature_with_space_at_start`",
        "-`feature_without_space_after_dash`",
        "- feature_without_start_backtick`",
        "- `feature_without_end_backtick",
    ]

    for line in invalid_lines:
        with pytest.raises(MetadataParsingException):
            get_feature_name(line, FEATURE_PATH)


def test_feature_regex_valid():
    valid_lines = ["- `{product}_feature_{time_window}_12`"]

    for line in valid_lines:
        get_feature_name(line, FEATURE_PATH)


def test_missing_metadata_header():
    cells = ["cell1", "# NoMetadataHere", "cell2"]
    with pytest.raises(MissingMetadataException):
        extract_metadata_string_from_cells(cells, FEATURE_PATH)


def test_metadata_integration(mocker):
    metadata_string = """
# Metadata
- category: `general`
## Features
- `{product}_feature_{time_window}`
  - description: `{product} feature in {time_window}`
  - tags: `tag1, tag2, tag3`
- `{product}_feature2_{time_window}`
  - description: `{product} feature in {time_window}`
  - tags: `tag1, tag2, tag3`
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

    metadata_string = extract_metadata_string_from_cells(["", metadata_string, ""], FEATURE_PATH)

    metadata = parse_metadata(metadata_string, FEATURE_PATH, df)

    feature_description_template = [
        ["product1_feature_14d", "product1 feature in 14 days", "{product}_feature_{time_window}"],
        ["product2_feature_14d", "product2 feature in 14 days", "{product}_feature_{time_window}"],
        ["product1_feature_30d", "product1 feature in 30 days", "{product}_feature_{time_window}"],
        ["product2_feature_30d", "product2 feature in 30 days", "{product}_feature_{time_window}"],
        ["product3_feature2_30d", "product3 feature in 30 days", "{product}_feature2_{time_window}"],
        ["product4_feature2_30d", "product4 feature in 30 days", "{product}_feature2_{time_window}"],
        ["product3_feature2_90d", "product3 feature in 90 days", "{product}_feature2_{time_window}"],
        ["product4_feature2_90d", "product4 feature in 90 days", "{product}_feature2_{time_window}"],
    ]

    expected_metadata = [
        {
            "feature": feature,
            "description": description,
            "feature_template": template,
            "description_template": "{product} feature in {time_window}",
            "dtype": "double",
            "category": "general",
            "tags": ["tag1", "tag2", "tag3"],
            "notebook_name": "feature",
            "notebook_absolute_path": FEATURE_PATH,
            "notebook_relative_path": RELATIVE_PATH,
        }
        for feature, description, template in feature_description_template
    ]

    assert metadata == expected_metadata
