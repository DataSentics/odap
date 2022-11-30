from typing import List
from odap.common.databricks import resolve_dbutils
from odap.common.widgets import get_widget_value
from odap.segment_factory.config import get_use_case_config, get_use_cases, get_exports

ALL = "all"
EXPORT_WIDGET = "export"
UC_EXPORT_SEPARATOR = ": "


def create_export_widget():
    dbutils = resolve_dbutils()

    exports: List[str] = [ALL]

    for use_case in get_use_cases():
        exports += [
            f"{use_case}{UC_EXPORT_SEPARATOR}{export}" for export in get_exports(get_use_case_config(use_case)).keys()
        ]

    dbutils.widgets.multiselect(EXPORT_WIDGET, ALL, exports)


def get_export_widget_value() -> List[str]:
    exports_str = get_widget_value(EXPORT_WIDGET)

    if exports_str == ALL:
        return [ALL]

    exports_list = exports_str.split(",")

    if ALL in exports_list:
        raise Exception(
            f"`{ALL}` together with selected exports is not a valid option. Please select "
            f"either `{ALL}` only or a subset of exports"
        )

    return exports_list
