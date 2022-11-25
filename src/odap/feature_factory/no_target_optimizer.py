from typing import List

import re

from pyspark.sql import SparkSession

from odap.feature_factory import const
from odap.common.widgets import get_widget_value
from odap.common.config import TIMESTAMP_COLUMN


def replace_sql_target_join(cell: str) -> str:
    found_join = re.search(const.SQL_TARGET_STORE_JOIN_REGEX, cell)
    found_select_start = re.search(const.SQL_SELECT_REGEX, cell)

    if found_join and found_select_start:
        cell = cell.replace(found_join.group(0), "")
        start = found_select_start.start(1)
        cell = cell[:start] + const.SQL_TIMESTAMP_LIT.format(timestamp=get_timestamp()) + cell[start:]

    return cell


def replace_pyspark_target_join(cell: str) -> str:
    return re.sub(const.PYTHON_TARGET_STORE_JOIN_REGEX, const.PYTHON_TIMESTAMP_LIT.format(timestamp=get_timestamp()), cell)


def replace_no_target(notebook_language: str, notebook_cells: List[str]):
    if get_widget_value(const.TARGET_WIDGET).strip() != const.NO_TARGET:
        return

    for idx, cell in enumerate(notebook_cells):
        if notebook_language == "SQL":
            notebook_cells[idx] = replace_sql_target_join(cell)
        elif notebook_language == "PYTHON":
            notebook_cells[idx] = replace_pyspark_target_join(cell)


def get_timestamp():
    spark = SparkSession.getActiveSession()

    return spark.read.table(const.TARGET_STORE).limit(1).select(TIMESTAMP_COLUMN).collect()[0][0]
