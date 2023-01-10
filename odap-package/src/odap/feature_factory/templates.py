import re
from copy import deepcopy
from typing import Any, Dict, List, Set
from pyspark.sql import DataFrame
from odap.common.config import TIMESTAMP_COLUMN, ConfigNamespace, get_config_namespace

from odap.feature_factory import const
from odap.feature_factory.config import get_entity_primary_key

from odap.feature_factory.metadata_schema import FeatureMetadataType, FeaturesMetadataType
from odap.feature_factory.time_windows import TIME_WINDOW_PLACEHOLDER, parse_time_window, is_time_window_parsable


def get_feature_placeholders(feature_name_template: str) -> List[str]:
    return re.findall(r"{(\w+)}", feature_name_template)


def get_feature_name_pattern(feature_name_template: str, placeholders: List[str]) -> re.Pattern:
    placeholder_translations = {placeholder: f"(?P<{placeholder}>.+)" for placeholder in placeholders}

    return re.compile(feature_name_template.format(**placeholder_translations))


def get_placeholder_to_value_dict(
    feature_name_pattern: re.Pattern, placeholders: List[str], feature_name: str
) -> Dict[str, str]:
    match = feature_name_pattern.fullmatch(feature_name)

    if not match:
        return {}

    return {placeholder: match.group(placeholder) for placeholder in placeholders}


def resolve_description(metadata_value: str, placeholder_to_value_dict: Dict[str, str]) -> str:
    copied_placehoder_to_value_dict = placeholder_to_value_dict.copy()

    if TIME_WINDOW_PLACEHOLDER in placeholder_to_value_dict and is_time_window_parsable(
        copied_placehoder_to_value_dict[TIME_WINDOW_PLACEHOLDER]
    ):
        description_time_window_dict = parse_time_window(copied_placehoder_to_value_dict[TIME_WINDOW_PLACEHOLDER])

        period, amount = next(iter(description_time_window_dict.items()))

        copied_placehoder_to_value_dict[TIME_WINDOW_PLACEHOLDER] = f"{amount} {period}"

    metadata_value = metadata_value.format(**copied_placehoder_to_value_dict)

    return metadata_value


def resolve_placeholders(
    feature_metadata: Dict[str, Any], placeholder_to_value_dict: Dict[str, str]
) -> FeatureMetadataType:
    new_metadata = deepcopy(feature_metadata)

    new_metadata[const.FEATURE] = new_metadata[const.FEATURE_TEMPLATE].format(**placeholder_to_value_dict)
    new_metadata[const.DESCRIPTION] = resolve_description(
        new_metadata[const.DESCRIPTION_TEMPLATE], placeholder_to_value_dict
    )
    new_metadata[const.EXTRA] = placeholder_to_value_dict

    return new_metadata


def resolve_placeholders_on_df_columns(
    df_columns: Set[str], feature_metadata: FeatureMetadataType, placeholders: List[str]
) -> FeaturesMetadataType:
    resolved_metadata = []

    feature_name_pattern = get_feature_name_pattern(feature_metadata[const.FEATURE_TEMPLATE], placeholders)

    for column_name in df_columns:
        placeholder_to_value_dict = get_placeholder_to_value_dict(feature_name_pattern, placeholders, column_name)

        if not placeholder_to_value_dict:
            continue

        resolved_metadata.append(resolve_placeholders(feature_metadata, placeholder_to_value_dict))

    return resolved_metadata


def resolve_metadata_template(feature_df: DataFrame, feature_metadata: FeatureMetadataType) -> FeaturesMetadataType:
    feature_metadata[const.FEATURE_TEMPLATE] = feature_metadata[const.FEATURE]
    feature_metadata[const.DESCRIPTION_TEMPLATE] = feature_metadata.get(const.DESCRIPTION, "")

    placeholders = get_feature_placeholders(feature_metadata[const.FEATURE])

    if not placeholders:
        return [feature_metadata]

    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

    resolvable_columns = set(feature_df.columns) - {get_entity_primary_key(config), TIMESTAMP_COLUMN}

    return resolve_placeholders_on_df_columns(resolvable_columns, feature_metadata, placeholders)


def resolve_metadata_templates(feature_df: DataFrame, features_metadata: FeaturesMetadataType) -> FeaturesMetadataType:
    resolved_metadata = []

    for metadata in features_metadata:
        resolved_metadata.extend(resolve_metadata_template(feature_df, metadata))

    return resolved_metadata
