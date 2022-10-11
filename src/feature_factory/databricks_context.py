import IPython
from pyspark.dbutils import DBUtils


def resolve_dbutils() -> DBUtils:
    ipython = IPython.get_ipython()

    if not hasattr(ipython, "user_ns") or "dbutils" not in ipython.user_ns:
        raise Exception("dbutils cannot be resolved")

    return ipython.user_ns["dbutils"]


def get_host(dbutils: DBUtils) -> str:
    return f"https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}"


def get_token(dbutils: DBUtils) -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
