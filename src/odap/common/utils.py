import os
import sys
import re
import importlib
from typing import Dict, List
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.workspace.api import WorkspaceFileInfo
from databricks_cli.repos.api import ReposApi


def get_repository_root_fs_path() -> str:
    return min((path for path in sys.path if path.startswith("/Workspace/Repos")), key=len)


def get_repository_root_api_path() -> str:
    return re.sub("^/Workspace", "", get_repository_root_fs_path())


def get_relative_path(path: str):
    return os.path.relpath(path, get_repository_root_api_path())


def get_notebook_name(path: str):
    return os.path.split(path)[-1]


def get_absolute_path(*paths: str) -> str:
    return os.path.join(get_repository_root_api_path(), *paths)


def import_file(module_name: str, file_path: str):
    module_name = f"odap_exporter_{module_name}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)  # pyre-ignore[6]
    sys.modules[spec.name] = module  # pyre-ignore[16]
    spec.loader.exec_module(module)  # pyre-ignore[16]
    return importlib.import_module(module_name)


def get_repository_info(workspace_api: WorkspaceApi, repos_api: ReposApi) -> Dict[str, str]:
    workspace_status = workspace_api.get_status(workspace_path=get_repository_root_api_path())
    return repos_api.get(workspace_status.object_id)


def list_notebooks_info(workspace_path: str, workspace_api: WorkspaceApi) -> List[WorkspaceFileInfo]:
    return [obj for obj in list_objects(workspace_path, workspace_api) if obj.object_type == "NOTEBOOK"]


def list_files(workspace_path: str, workspace_api: WorkspaceApi) -> List[WorkspaceFileInfo]:
    return [obj for obj in list_objects(workspace_path, workspace_api) if obj.object_type == "FILE"]


def list_objects(workspace_path: str, workspace_api: WorkspaceApi) -> List[WorkspaceFileInfo]:
    objects_to_return = []

    objects = workspace_api.list_objects(workspace_path)

    for obj in objects:
        if obj.is_dir:
            objects_to_return += list_objects(obj.path, workspace_api)

        else:
            objects_to_return += [obj]

    return objects_to_return


def string_contains_any_pattern(string: str, patterns: List[str]):
    for pattern in patterns:
        if re.search(pattern, string):
            return True

    return False
