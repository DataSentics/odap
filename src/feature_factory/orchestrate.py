from feature_factory.config import get_config, get_features_table, get_features_table_path, get_entity_primary_key
from feature_factory.dataframes import create_dataframes, join_dataframes


def orchestrate():

    config = get_config()

    dataframes = create_dataframes()

    features_df = join_dataframes(dataframes, join_columns=[get_entity_primary_key(config)])

    (
        features_df.write.mode("overwrite")
        .option("path", get_features_table_path(config))
        .option("overwriteSchema", True)
        .saveAsTable(get_features_table(config))
    )
