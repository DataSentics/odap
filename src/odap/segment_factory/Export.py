from pyspark.sql import DataFrame, SparkSession

from odap.common.config import get_config_namespace, ConfigNamespace
from odap.feature_factory.config import get_entity_by_name, get_latest_features_table_for_entity
from odap.segment_factory.config import get_destination, get_export, get_use_case_config
from odap.segment_factory.exporters import resolve_exporter
from odap.segment_factory.logs import write_export_log
from odap.segment_factory.schemas import SEGMENT
from odap.segment_factory.segments import create_segments_union_df
from odap.common.logger import logger


class Export:
    def __init__(self, use_case_name: str, export_name: str):
        self._use_case_name = use_case_name
        self._export_name = export_name
        self._segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)
        self._use_case_config = get_use_case_config(use_case_name)
        self._export_config = get_export(export_name, self._use_case_config)
        self._destination_config = get_destination(self._export_config["destination"], self._segment_factory_config)

        def export_function(df: DataFrame):
            export = resolve_exporter(self._destination_config["type"])
            return export(self._export_name, df, self._export_config, self._destination_config)

        self._export_function = export_function

    def _join_segment_with_entities(self, segment_df: DataFrame) -> DataFrame:
        spark = SparkSession.getActiveSession()
        feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

        for entity_name in self._destination_config.get("attributes"):
            id_column = get_entity_by_name(entity_name, feature_factory_config).get("id_column")

            latest_features_df = spark.read.table(
                get_latest_features_table_for_entity(entity_name, feature_factory_config)
            )

            if id_column not in segment_df.columns:
                raise Exception(f"'{id_column}' column is missing in the segment dataframe")

            if id_column not in latest_features_df.columns:
                raise Exception(
                    f"'{id_column}' column is missing in the latest features dataframe for entity '{entity_name}'"
                )

            segment_df = segment_df.join(latest_features_df, id_column, "inner")

        return segment_df

    def _select_attributes(self, df: DataFrame) -> DataFrame:
        select_columns = [
            attribute for attributes in self._destination_config.get("attributes").values() for attribute in attributes
        ]
        return df.select(SEGMENT, *select_columns)

    @property
    def export_function(self):
        return self._export_function

    def get_united_df(self) -> DataFrame:
        united_segments_df = create_segments_union_df(self._export_config["segments"], self._use_case_name)
        return united_segments_df

    def get_export_df(self) -> DataFrame:
        united_segments_df = self.get_united_df()
        joined_segment_feature_stores_df = self._join_segment_with_entities(united_segments_df)
        return self._select_attributes(joined_segment_feature_stores_df)

    def run(self):
        logger.info(f"Running export {self._use_case_name}/{self._export_name}")

        united_segments_df = self.get_united_df()
        final_export_df = self.get_export_df()

        self._export_function(final_export_df)

        write_export_log(
            united_segments_df,
            self._export_name,
            self._use_case_name,
            self._export_config,
            self._segment_factory_config,
        )

        logger.info(f"Export {self._use_case_name}/{self._export_name} done")
