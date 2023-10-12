# Databricks notebook source
# MAGIC %pip install "setuptools<58"

# COMMAND ----------

# MAGIC %pip install "loguru==0.7.*"

# COMMAND ----------

import os
import sys

# COMMAND ----------

def resolve_repository_root():
    return min([path for path in sys.path if path.startswith("/Workspace/Repos")], key=len)

# COMMAND ----------

sys.path.append(f"{resolve_repository_root()}/odap-package/src")

# COMMAND ----------

sys.path.append(f"{resolve_repository_root()}/odap_framework_demo")
os.environ["ODAP_BASE_DIR"] = "odap_framework_demo"
