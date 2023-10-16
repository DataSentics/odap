# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Orchestration

# COMMAND ----------

# MAGIC %run ../init/odap

# COMMAND ----------

from odap.feature_factory.imports import (
    create_notebooks_widget,
    orchestrate,
    calculate_latest_table,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text target default "no target";
# MAGIC create widget text timestamp default "2020-12-12";
# MAGIC create widget text timeshift default "0"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook selector

# COMMAND ----------

create_notebooks_widget()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize target store

# COMMAND ----------

# MAGIC %run ../init/target_store

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize SQL window functions

# COMMAND ----------

# MAGIC %run ../init/window_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orchestrate

# COMMAND ----------

orchestrate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate latest features cache

# COMMAND ----------

calculate_latest_table()
