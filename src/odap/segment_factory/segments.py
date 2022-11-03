from pyspark.sql import DataFrame
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.utils import get_absolute_path, get_notebook_cells
from odap.segment_factory.exceptions import SegmentNotFoundException


def create_segment_dataframe_by_slug(slug: str) -> DataFrame:
    workspace_api = get_workspace_api()

    segment_path = get_absolute_path("segments", slug)

    notebook_cells = get_notebook_cells(segment_path, workspace_api)
    segment_df = create_dataframe_from_notebook_cells(segment_path, notebook_cells)

    if not segment_df:
        raise SegmentNotFoundException(f"Segment '{slug}' could not be loaded")
    return segment_df
