from typing import Dict
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_export, get_flatten_segments_exports, get_segment
from odap.segment_factory.dataframes import create_export_dataframe
from odap.segment_factory.exporters import resolve_exporter
from odap.segment_factory.segments import create_segment_dataframe_by_slug


# pylint: disable=too-many-statements
def run_export(segment_name: str, export_name: str, feature_factory_config: Dict, segment_factory_config: Dict):
    segment_config = get_segment(segment_name, segment_factory_config)
    export_config = get_export(export_name, segment_factory_config)

    segment_dataframe = create_segment_dataframe_by_slug(segment_name)

    export_dataframe = create_export_dataframe(segment_dataframe, export_config, feature_factory_config)

    exporter_fce = resolve_exporter(export_config["type"])
    exporter_fce(segment_name, export_dataframe, segment_config, export_config)


def run_exports():
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    for segment_name, export_name in get_flatten_segments_exports(segment_factory_config):
        run_export(segment_name, export_name, feature_factory_config, segment_factory_config)
