from typing import Dict, List
from odap.segment_factory.config import get_exports, get_use_cases, get_use_case_config
from odap.segment_factory.Export import Export
from odap.common.logger import logger
from odap.segment_factory.widgets import ALL, UC_EXPORT_SEPARATOR

# pylint: disable=too-many-statements
def create_use_case_export_map(use_case_exports_list: List[str]) -> Dict[str, List[str]]:
    result_dict: Dict[str, List[str]] = {}

    use_cases = get_use_cases()

    if ALL in use_case_exports_list:
        for use_case in use_cases:
            use_case_config = get_use_case_config(use_case)
            result_dict[use_case] = list(get_exports(use_case_config).keys())
        return result_dict

    if any(UC_EXPORT_SEPARATOR not in item for item in use_case_exports_list):
        raise Exception("Invalid export name. The export name must contain a ': ' delimiter.")

    for use_case_export in use_case_exports_list:
        use_case, export = use_case_export.split(UC_EXPORT_SEPARATOR)

        if use_case not in use_cases:
            raise Exception(f"'{use_case}' is not a valid use case name.")
        result_dict.setdefault(use_case, []).append(export)

    return result_dict


def orchestrate_use_case(
    use_case_name: str,
    selected_exports: List[str],
):
    logger.info(f"Running {use_case_name} use case")

    for export_name in selected_exports:
        Export(use_case_name, export_name).run()
