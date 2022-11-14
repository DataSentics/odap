from typing import Any, List, Dict
from databricks_cli.workspace.api import WorkspaceFileInfo, WorkspaceApi

from odap.feature_factory import const

from odap.common.logger import logger
from odap.common.config import Config
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.notebook import eval_cell_with_header, get_notebook_cells

from odap.common.utils import get_absolute_path
from odap.common.utils import list_notebooks_info
from odap.feature_factory.config import get_entity_primary_key
from odap.feature_factory.dataframes.dataframe_checker import check_feature_df
from odap.feature_factory.metadata import resolve_metadata, set_fs_compatible_metadata
from odap.feature_factory.no_target_optimizer import replace_no_target


class FeatureNotebook:
    def __init__(self, notebook_info: WorkspaceFileInfo, config: Dict[str, Any], workspace_api: WorkspaceApi):
        self.info = notebook_info
        self.cells = self.get_feature_notebook_cells(notebook_info, workspace_api)
        self.df = create_dataframe_from_notebook_cells(self.info, self.cells[:])
        self.metadata = resolve_metadata(self.cells, self.info.path, self.df)

        self.post_load_actions(config)

    def post_load_actions(self, config: Config):
        entity_primary_key = get_entity_primary_key(config)

        self.df = self.fill_nulls()
        set_fs_compatible_metadata(self.metadata, config)

        check_feature_df(self.df, entity_primary_key, self.metadata, self.info.path)

        logger.info(f"Feature {self.info.path} successfully loaded.")

    def fill_nulls(self):
        fill_dict = {
            feature[const.FEATURE]: feature[const.FILLNA_VALUE]
            for feature in self.metadata
            if feature[const.FILLNA_VALUE] is not None
        }

        return self.df.fillna(fill_dict)

    def get_feature_notebook_cells(self, info: WorkspaceFileInfo, workspace_api: WorkspaceApi):
        notebook_cells = get_notebook_cells(info, workspace_api)
        replace_no_target(info.language, notebook_cells)
        return notebook_cells

    def get_dq_checks_list(self) -> List[str]:
        checks_list = eval_cell_with_header(self.cells, self.info.path, const.DQ_CHECKS_HEADER_REGEX, const.DQ_CHECKS)

        return checks_list or []


FeatureNotebooks = List[FeatureNotebook]


def get_feature_notebooks_info(workspace_api: WorkspaceApi) -> List[WorkspaceFileInfo]:
    features_path = get_absolute_path("features")

    return list_notebooks_info(features_path, workspace_api)


def load_feature_notebooks(config: Config, notebooks_info: List[WorkspaceFileInfo]) -> FeatureNotebooks:
    workspace_api = get_workspace_api()

    feature_notebooks = []

    for info in notebooks_info:
        feature_notebooks.append(FeatureNotebook(info, config, workspace_api))

    return feature_notebooks
