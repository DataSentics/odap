from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_flatten_segments_exports
from odap.segment_factory.exports import run_export


def run_exports():
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    for segment_name, export_name in get_flatten_segments_exports(segment_factory_config):
        run_export(segment_name, export_name, feature_factory_config, segment_factory_config)


def orchestrate():
    run_exports()
