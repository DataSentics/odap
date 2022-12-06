from odap.common.config import get_config_namespace, ConfigNamespace
from odap.segment_factory.widgets import get_export_widget_value
from odap.segment_factory.use_cases import orchestrate_use_case, create_use_case_export_map


def orchestrate():
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    selected_exports = get_export_widget_value()
    use_case_export_map = create_use_case_export_map(selected_exports)

    for use_case, exports in use_case_export_map.items():
        orchestrate_use_case(use_case, exports, feature_factory_config, segment_factory_config)
