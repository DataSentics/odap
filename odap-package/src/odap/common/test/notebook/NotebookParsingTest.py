import os
import datetime as dt
import unittest
from unittest.mock import MagicMock, patch
from odap.common.test.PySparkTestCase import PySparkTestCase
from odap.common.notebook import split_notebok_to_cells
from odap.common.dataframes import create_dataframe_from_notebook_cells


class WorkspaceFileInfo:
    def __init__(self, path, language):
        self.path = path
        self.language = language


class NotebookParsingTest(PySparkTestCase):
    @patch("odap.common.dataframes.resolve_dbutils", return_value=MagicMock())
    @patch("odap.common.dataframes.resolve_display", return_value=MagicMock())
    @patch("odap.common.dataframes.resolve_display_html", return_value=MagicMock())
    def test_python_notebook(self, *_):
        with open(os.path.join(os.path.dirname(__file__), "python_notebook.txt"), mode="r", encoding="utf-8") as f:
            notebook_content = f.read()

        notebook_info = WorkspaceFileInfo("/dummy/path", "PYTHON")
        notebook_cells = split_notebok_to_cells(notebook_content, notebook_info)

        df_final = create_dataframe_from_notebook_cells(notebook_info, notebook_cells)

        expected_df = self.spark.createDataFrame(
            [[1, dt.datetime(2020, 12, 12), 0, 0, 0, 0, 0, 0, 0, 0, 0]],
            [
                "customer_id",
                "timestamp",
                "investice_web_visits_count_in_last_14d",
                "pujcky_web_visits_count_in_last_14d",
                "hypoteky_web_visits_count_in_last_14d",
                "investice_web_visits_count_in_last_30d",
                "pujcky_web_visits_count_in_last_30d",
                "hypoteky_web_visits_count_in_last_30d",
                "investice_web_visits_count_in_last_90d",
                "pujcky_web_visits_count_in_last_90d",
                "hypoteky_web_visits_count_in_last_90d",
            ],
        )

        self.compare_dataframes(df_final, expected_df, ["customer_id"])

    @patch("odap.common.dataframes.resolve_dbutils", return_value=MagicMock())
    @patch("odap.common.dataframes.resolve_display", return_value=MagicMock())
    @patch("odap.common.dataframes.resolve_display_html", return_value=MagicMock())
    def test_sql_notebook(self, *_):
        with open(os.path.join(os.path.dirname(__file__), "sql_notebook.txt"), mode="r", encoding="utf-8") as f:
            notebook_content = f.read()

        notebook_info = WorkspaceFileInfo("/dummy/path", "SQL")
        notebook_cells = split_notebok_to_cells(notebook_content, notebook_info)

        df_final = create_dataframe_from_notebook_cells(notebook_info, notebook_cells)

        expected_df = self.spark.createDataFrame(
            [[1, dt.datetime(2020, 12, 12), 201.5]], ["customer_id", "timestamp", "transactions_sum_amount_in_last_30d"]
        )

        self.compare_dataframes(df_final, expected_df, ["customer_id"])


if __name__ == "__main__":
    unittest.main()
