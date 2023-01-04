from odap.common.databricks import resolve_dbutils


def get_widget_value(widget_name: str) -> str:
    dbutils = resolve_dbutils()

    return dbutils.widgets.get(widget_name)
