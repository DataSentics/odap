from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.config import get_exports
from odap.segment_factory.exports import run_export


def run_exports():
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    for export_name in get_exports(segment_factory_config).keys():
        run_export(export_name, feature_factory_config, segment_factory_config)


def orchestrate():
    run_exports()
