import re
from typing import Callable, Dict
from pyspark.sql import DataFrame
from odap.common.utils import get_absolute_path, import_file
from odap.common.databricks_context import get_workspace_api, resolve_dbutils

ExportersMap = Dict[str, str]


def load_exporters_map() -> ExportersMap:
    dbutils = resolve_dbutils()
    workspace_api = get_workspace_api(dbutils)
    exporters_path = get_absolute_path("exporters")

    exporters = {}
    for exporter in workspace_api.list_objects(exporters_path):
        if not exporter.is_dir:
            name = re.sub(".py$", "", exporter.basename)
            exporter_fs_path = re.sub("^/Repos", "/Workspace/Repos", exporter.path)
            exporters[name] = exporter_fs_path
    return exporters


def resolve_exporter(exporter_name: str, exporters_map: ExportersMap) -> Callable[[str, DataFrame, Dict, Dict], None]:
    if not exporter_name in exporters_map:
        raise Exception(f"Exporter '{exporter_name}' is not implemented.")

    exporter_module = import_file(exporter_name, exporters_map[exporter_name])

    if not hasattr(exporter_module, "export"):
        raise Exception(f"Function 'export' is not declared in module '{exporter_name}.py'.")

    return exporter_module.export