# Databricks notebook source
# MAGIC %pip install "setuptools<58"

# COMMAND ----------

import sys

# COMMAND ----------

def resolve_repository_root():
    return min([path for path in sys.path if path.startswith("/Workspace/Repos")], key=len)

# COMMAND ----------

sys.path.append(f"{resolve_repository_root()}/odap-package/src")
