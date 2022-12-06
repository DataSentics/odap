from odap.segment_factory.widgets import get_export_widget_value
from odap.segment_factory.use_cases import orchestrate_use_case, create_use_case_export_map


def orchestrate():
    selected_exports = get_export_widget_value()
    use_case_export_map = create_use_case_export_map(selected_exports)

    for use_case_name, exports in use_case_export_map.items():
        orchestrate_use_case(use_case_name, exports)
