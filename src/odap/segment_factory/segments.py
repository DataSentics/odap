from typing import Dict, Any
from pyspark.sql import DataFrame
from odap.common.databricks_context import get_workspace_api, resolve_dbutils
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.utils import get_absolute_path, get_notebook_cells
from odap.segment_factory.config import get_segment_table_path, get_segment_table
from odap.segment_factory.exceptions import SegmentNotFoundException


def create_segment_dataframe_by_slug(slug: str) -> DataFrame:
    dbutils = resolve_dbutils()
    workspace_api = get_workspace_api(dbutils)

    segment_path = get_absolute_path("segments", slug)

    notebook_cells = get_notebook_cells(segment_path, workspace_api)
    segment_df = create_dataframe_from_notebook_cells(segment_path, notebook_cells)

    if not segment_df:
        raise SegmentNotFoundException(f"Segment '{slug}' could not be loaded")
    return segment_df


def save_segment_dataframe(df: DataFrame, slug: str, config: Dict[str, Any]) -> None:
    segment_db_path = get_segment_table_path(slug, config)
    segment_db_name = get_segment_table(slug, config)

    df.write.mode("overwrite").option("overwriteSchema", True).option("path", segment_db_path).saveAsTable(
        segment_db_name
    )
