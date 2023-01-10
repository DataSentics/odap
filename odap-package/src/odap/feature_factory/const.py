# Widgets
TIMESTAMP_WIDGET = "timestamp"
TARGET_WIDGET = "target"
FEATURE_WIDGET = "feature"
DISPLAY_WIDGET = "display"

NO_TARGET = "no target"
ALL_FEATURES = "all"
DISPLAY_METADATA = "Display Metadata"
DISPLAY_FEATURES = "Display Features"


# Regex
DQ_CHECKS_HEADER_REGEX = r"dq_checks\s*=\s*\["
METADATA_HEADER_REGEX = r"metadata\s*=\s*{"

SQL_TARGET_STORE_JOIN_REGEX = r"join\s*target_store\s*using\s*\(.*\)\s"
SQL_SELECT_REGEX = r"select([\s\S]*)from"

PYTHON_TARGET_STORE_JOIN_REGEX = r"\.join\(\s*target_store[^.]*\."


# Metadata table columns
FEATURE = "feature"
DESCRIPTION = "description"
EXTRA = "extra"
FEATURE_TEMPLATE = "feature_template"
DESCRIPTION_TEMPLATE = "description_template"
CATEGORY = "category"
OWNER = "owner"
TAGS = "tags"
START_DATE = "start_date"
FREQUENCY = "frequency"
LAST_COMPUTE_DATE = "last_compute_date"
DTYPE = "dtype"
VARIABLE_TYPE = "variable_type"
FILLNA_VALUE = "fillna_value"
FILLNA_VALUE_TYPE = "fillna_value_type"
NOTEBOOK_NAME = "notebook_name"
NOTEBOOK_ABSOLUTE_PATH = "notebook_absolute_path"
NOTEBOOK_RELATIVE_PATH = "notebook_relative_path"
TABLE = "table"
BACKEND = "backend"

# Other

FILLNA_WITH = "fillna_with"

METADATA = "metadata"
DQ_CHECKS = "dq_checks"

TARGET_STORE = "target_store"

SQL_TIMESTAMP_LIT = ' timestamp("{timestamp}") as timestamp,'
PYTHON_TIMESTAMP_LIT = '.withColumn("timestamp", f.lit("{timestamp}").cast("timestamp")).'
