from typing import Dict
from odap.common.config import get_config_on_rel_path, get_config_namespace, ConfigNamespace, CONFIG_NAME
from odap.segment_factory.config import get_exports, get_use_cases, USE_CASES_FOLDER
from odap.segment_factory.exports import run_export
from odap.common.logger import logger


def orchestrate_use_case(use_case: str, feature_factory_config: Dict, segment_factory_config: Dict):
    logger.info(f"Running {use_case} use case")

    use_case_config = get_config_on_rel_path(USE_CASES_FOLDER, use_case, CONFIG_NAME)

    for export_name in get_exports(use_case_config).keys():
        run_export(
            export_name,
            use_case,
            use_case_config,
            feature_factory_config,
            segment_factory_config,
        )


def orchestrate():
    feature_factory_config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    segment_factory_config = get_config_namespace(ConfigNamespace.SEGMENT_FACTORY)

    use_cases = get_use_cases()

    for use_case in use_cases:
        orchestrate_use_case(use_case, feature_factory_config, segment_factory_config)
