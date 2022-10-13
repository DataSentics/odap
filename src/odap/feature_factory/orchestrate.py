from odap.common.config import get_config_namespace, ConfigNamespace
from odap.feature_factory.config import get_features_table, get_features_table_path, get_entity_primary_key
from odap.feature_factory.dataframes import create_dataframes, join_dataframes
from odap.feature_factory.feature_store import write_df_to_feature_store


def orchestrate():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    entity_primary_key = get_entity_primary_key(config)

    dataframes = create_dataframes()

    df = join_dataframes(dataframes, join_columns=[entity_primary_key])

    table_name = get_features_table(config)
    table_path = get_features_table_path(config)

    write_df_to_feature_store(df, table_name, table_path, primary_key=entity_primary_key)
