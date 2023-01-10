from typing import List
import uuid
import yaml

from pyspark.sql import SparkSession, DataFrame

from odap.feature_factory.feature_notebook import FeatureNotebookList
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
    _id = "id" + uuid.uuid4().hex  # id has to start with letter
    df.createOrReplaceTempView(_id)
    return _id


def execute_soda_checks_from_feature_notebooks(df: DataFrame, feature_notebooks: FeatureNotebookList):
    checks_list = []

    for notebook in feature_notebooks:
        checks_list += notebook.df_checks

    if checks_list:
        execute_soda_checks(df, checks_list)


def execute_soda_checks(df: DataFrame, checks_list: List[str]):
    if not soda_is_installed():
        return

    table_id = create_temporary_view(df)
    yaml_checks = yaml.dump({f"checks for {table_id}": checks_list})

    scan = get_soda_scan()
    scan.add_sodacl_yaml_str(yaml_checks)

    logger.info("Starting soda data quality checks:\n")

    scan.execute()

    resolve_scan(scan, table_id)


def resolve_scan(scan, table_id: str):
    print(scan.get_logs_text())

    SparkSession.getActiveSession().catalog.dropTempView(table_id)

    if scan.has_check_fails():
        logger.error("Soda data quality checks failed!")
        scan.assert_no_checks_fail()
