import os
import enum
import hashlib
from typing import Dict
from pyspark.sql import DataFrame
from odap.common.config import get_config_namespace

class ConfigNamespace(enum.Enum):
    P360_EXPORT = "p360_export"

def export(export_name: str, segment_df: DataFrame, export_config: Dict, destination_config: Dict):
    try:
        from p360_export.ExportRunner import odap_export
    except ModuleNotFoundError:
        raise Exception("Module 'p360_export' not installed! To use p360_export add 'p360_export' to requirements.txt")

    entities = destination_config.get('attributes')

    export_columns = [attr for attrs in entities.values() for attr in attrs]
    config = {
        'destination_type': destination_config["p360_destination"],
        'credentials': destination_config["credentials"],
        'params': {
            'export_columns': export_columns,
            'mapping': destination_config["mapping"]
        },
        'export_title': export_name,
        'export_id': hashlib.md5(export_name.encode('utf-8')).hexdigest(), # TODO id should contain use_case name
        'p360_export': get_config_namespace(ConfigNamespace.P360_EXPORT)
    }
    
    odap_export(segment_df, config)
