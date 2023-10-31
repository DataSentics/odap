# Databricks notebook source
# MAGIC %run ../init/odap

# COMMAND ----------

from odap.segment_factory.widgets import create_export_widget

create_export_widget()

# COMMAND ----------

from odap.segment_factory.orchestrate import orchestrate

orchestrate()
