from odap.common.databricks import resolve_dbutils

TIMESTAMP_WIDGET = "timestamp"
TARGET_WIDGET = "target"
FEATURE_WIDGET = "feature"

NO_TARGET = "no target"
ALL_FEATURES = "all"


def get_widget_value(widget_name: str) -> str:
    dbutils = resolve_dbutils()

    return dbutils.widgets.get(widget_name)