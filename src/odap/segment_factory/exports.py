from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import (
    get_export,
    get_flatten_segments_exports,
    get_segment,
    get_segments_export,
)
from odap.segment_factory.dataframes import create_segment_export_dataframe
from odap.segment_factory.exporters import resolve_exporter, load_exporters_map


# pylint: disable=too-many-statements
def run_export(segment_name: str, export_name: str):
    config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    export_name = get_segments_export(segment_name, export_name, config)

    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    export_df = create_segment_export_dataframe(
        segment_name,
        export_name,
        feature_factory_config,
        segment_factory_config,
    )

    segment_config = get_segment(segment_name, segment_factory_config)
    export_config = get_export(export_name, segment_factory_config)

    exporters_map = load_exporters_map()
    exporter_fce = resolve_exporter(export_config["type"], exporters_map)

    exporter_fce(segment_name, export_df, segment_config, export_config)


def run_exports():
    config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)
    flatten_segments_exports = get_flatten_segments_exports(config)

    for segment_export in flatten_segments_exports:
        run_export(*segment_export)
