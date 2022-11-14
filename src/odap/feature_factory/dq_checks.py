from typing import List
import secrets
import yaml

from pyspark.sql import SparkSession, DataFrame

from odap.feature_factory.feature_notebook import FeatureNotebooks
from odap.common.logger import logger


def soda_is_installed():
    try:
        import soda  # pylint: disable=import-outside-toplevel,unused-import

        return True
    except ModuleNotFoundError:
        logger.warning(
            "Module soda not installed! If you want to run data quality checks, install it by running: '%pip install soda-core-spark-df'"
        )

    return False


def get_soda_scan():
    from soda.scan import Scan  # pylint: disable=import-outside-toplevel

    scan = Scan()
    scan.add_spark_session(SparkSession.getActiveSession())
    scan.set_data_source_name("spark_df")

    return scan


def create_temporary_view(df: DataFrame) -> str:
    _id = secrets.token_hex(8)
    df.createOrReplaceTempView(_id)
    return _id


def create_yaml_checks(df: DataFrame, checks_list: List[str]) -> str:
    table_id = create_temporary_view(df)
    checks = {f"checks for {table_id}": checks_list}
    return yaml.dump(checks)


def execute_soda_checks_from_feature_notebooks(df: DataFrame, feature_notebooks: FeatureNotebooks):
    checks_list = []

    for notebook in feature_notebooks:
        checks_list += notebook.get_dq_checks_list()

    if checks_list:
        execute_soda_checks(df, checks_list)


def execute_soda_checks(df: DataFrame, checks_list: List[str]):
    if not soda_is_installed():
        return

    yaml_checks = create_yaml_checks(df, checks_list)

    scan = get_soda_scan()
    scan.add_sodacl_yaml_str(yaml_checks)

    scan.execute()

    print(scan.get_logs_text())
