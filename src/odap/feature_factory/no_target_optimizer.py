from typing import List

import re
from odap.common.widgets import NO_TARGET, TARGET_WIDGET, get_widget_value

SQL_TARGET_STORE_JOIN_REGEX = r"join\s*target_store\s*using\s*\(.*\)\s"
SQL_SELECT_REGEX = r"select([\s\S]*)from"
SQL_TIMESTAMP_LIT = ' timestamp(getargument("timestamp")) as timestamp,'

PYTHON_TARGET_STORE_JOIN_REGEX = r"\.join\(\s*target_store[^.]*\."
PYTHON_TIMESTAMP_LIT = '.withColumn("timestamp", f.lit(dbutils.widgets.get("timestamp")).cast("timestamp")).'


def replace_sql_target_join(cell: str) -> str:
    found_join = re.search(SQL_TARGET_STORE_JOIN_REGEX, cell)
    found_select_start = re.search(SQL_SELECT_REGEX, cell)

    if found_join and found_select_start:
        cell = cell.replace(found_join.group(0), "")
        start = found_select_start.start(1)
        cell = cell[:start] + SQL_TIMESTAMP_LIT + cell[start:]

    return cell


def replace_pyspark_target_join(cell: str) -> str:
    return re.sub(PYTHON_TARGET_STORE_JOIN_REGEX, PYTHON_TIMESTAMP_LIT, cell)


def replace_no_target(notebook_language: str, notebook_cells: List[str]):
    if get_widget_value(TARGET_WIDGET).strip() != NO_TARGET:
        return

    for idx, cell in enumerate(notebook_cells):
        if notebook_language == "SQL":
            notebook_cells[idx] = replace_sql_target_join(cell)
        elif notebook_language == "PYTHON":
            notebook_cells[idx] = replace_pyspark_target_join(cell)
