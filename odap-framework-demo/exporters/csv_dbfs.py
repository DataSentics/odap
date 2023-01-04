import os
from typing import Dict
from pyspark.sql import SparkSession, DataFrame, functions as f


def export(export_name: str, segment_df: DataFrame, export_config: Dict, destination_config: Dict):
    """Simple exporter example
    Export DF as CSV to storage.

    Parameters
    ----------
    export_name : str
        The name of the export
    segment_df : DataFrame
        Segment dataframe
    segment_config : Dict
        The segment configuration specified in the config.yaml file.
    exporter_config : Dict
        The exporter configuration specified in the config.yaml file.
    """

    spark = SparkSession.getActiveSession()
    
    output_path = destination_config["path"]
    output_blob_path = f"/dbfs/odap_exports/{output_path}/{export_name}.csv"

    os.makedirs(os.path.dirname(output_blob_path), exist_ok=True)
    
    segments_config = export_config["segments"]

    names_dictionary_df = spark.createDataFrame(
        map(lambda k, v: [k, v["name"]], segments_config.keys(), segments_config.values()),
        ["segment", "name"]
    )

    (
        segment_df
         .join(names_dictionary_df, "segment", "outer")
         .select("name", *segment_df.columns)
         .toPandas().to_csv(output_blob_path, index=False)
    )
