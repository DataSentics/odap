from typing import List, Dict
from databricks_cli.workspace.api import WorkspaceFileInfo, WorkspaceApi
from pyspark.sql import DataFrame

from odap.feature_factory import const

from odap.common.logger import logger
from odap.common.config import Config
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe_from_notebook_cells
from odap.common.notebook import eval_cell_with_header, get_notebook_cells

from odap.common.utils import get_absolute_api_path
from odap.common.utils import list_notebooks_info
from odap.feature_factory.config import get_entity_primary_key, use_no_target_optimization
from odap.feature_factory.dataframes.dataframe_checker import check_feature_df
from odap.feature_factory.metadata import resolve_metadata, set_fs_compatible_metadata
from odap.feature_factory.metadata_schema import FeaturesMetadataType
from odap.feature_factory.no_target_optimizer import replace_no_target


from odap.feature_factory.config import get_feature_dir
from odap.common.config import get_config_namespace, ConfigNamespace


class FeatureNotebook:
    def __init__(
        self,
        notebook_info: WorkspaceFileInfo,
        df: DataFrame,
        metadata: FeaturesMetadataType,
        config: Config,
        df_checks: List[str],
    ):
        self.info = notebook_info
        self.df = df
        self.metadata = metadata
        self.df_checks = df_checks

        self.post_load_actions(config)

    def __iter__(self):
        yield self.df
        yield self.metadata
        yield self.df_checks

    @classmethod
    def from_api(cls, notebook_info: WorkspaceFileInfo, config: Config, workspace_api: WorkspaceApi):

        entity_primary_key = get_entity_primary_key(config)
        info = notebook_info
        cells = get_feature_notebook_cells(notebook_info, workspace_api, config)
        feature_path = notebook_info.path
        prefix = get_prefix_for_feature(feature_path)
        df = create_dataframe_with_prefix(info, cells, prefix, entity_primary_key)
        metadata = resolve_metadata(cells, info.path, df, prefix)
        df_check_list = get_dq_checks_list(info, cells)

        return cls(info, df, metadata, config, df_check_list)

    def post_load_actions(self, config: Config):
        entity_primary_key = get_entity_primary_key(config)

        set_fs_compatible_metadata(self.metadata, config)

        check_feature_df(self.df, entity_primary_key, self.metadata, self.info.path)

        logger.info(f"Feature {self.info.path} successfully loaded.")


FeatureNotebookList = List[FeatureNotebook]


def get_prefix_for_feature(feature_path):
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    feature_dirs = get_feature_dir(config)
    prefix = ""

    for repo in feature_dirs:
        path = repo.get("path", "")

        if path in feature_path:
            prefix = repo.get("prefix", "")
            break

    return prefix


def create_dataframe_with_prefix(info, cells, prefix, entity_primary_key):
    if prefix:
        df_with_prefix = create_dataframe_from_notebook_cells(info, cells[:], prefix)
        df = df_with_prefix.withColumnRenamed(f"{prefix}_{entity_primary_key}", entity_primary_key).withColumnRenamed(
            f"{prefix}_timestamp", "timestamp"
        )

    else:
        df = create_dataframe_from_notebook_cells(info, cells[:])

    return df


def get_feature_notebooks_info(workspace_api: WorkspaceApi, feature_dir: str) -> List[WorkspaceFileInfo]:
    features_path = get_absolute_api_path(feature_dir)

    return list_notebooks_info(features_path, workspace_api, recurse=True)


def get_feature_notebook_cells(info: WorkspaceFileInfo, workspace_api: WorkspaceApi, config: Config) -> List[str]:
    notebook_cells = get_notebook_cells(info, workspace_api)

    if use_no_target_optimization(config):
        replace_no_target(info.language, notebook_cells)
    return notebook_cells


def load_feature_notebooks(config: Config, notebooks_info: List[WorkspaceFileInfo]) -> FeatureNotebookList:
    workspace_api = get_workspace_api()

    feature_notebooks = []

    for info in notebooks_info:
        feature_notebooks.append(FeatureNotebook.from_api(info, config, workspace_api))

    return feature_notebooks


def create_notebook_table_mapping(feature_notebooks: FeatureNotebookList) -> Dict[str, FeatureNotebookList]:
    mapping = {}
    list_feature_notebooks = [item for sublist in feature_notebooks for item in sublist]

    for feature_notebook in list_feature_notebooks:
        table = feature_notebook.metadata[0].get("table", None)

        if table not in mapping:
            mapping[table] = []

        mapping[table].append(feature_notebook)
    return mapping


def get_dq_checks_list(info, cells) -> List[str]:
    checks_list = eval_cell_with_header(cells, info.path, const.DQ_CHECKS_HEADER_REGEX, const.DQ_CHECKS)

    return checks_list or []
