# Databricks notebook source
# MAGIC %md
# MAGIC # ODAP Framework demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC 
# MAGIC Documentation for ODAP Feature factory, Segment factory and Use case factory can be found [here](https://datasentics.notion.site/ODAP-Use-Case-Framework-f6ed0a95140d48c69b642b568c6db85f)
# MAGIC 
# MAGIC Useful links:
# MAGIC 
# MAGIC 1. [Create a new project](https://datasentics.notion.site/Create-a-new-project-3bbc57914dfa42b9be77b5fcde162b8d)
# MAGIC 1. [Feature factory overview](https://www.notion.so/datasentics/Feature-factory-overview-d0082b2f128c4d2db5e8e4e1ed95b038?pvs=4)
# MAGIC 1. [Segment factory overview](https://datasentics.notion.site/Segments-factory-overview-f0c8ee49eecf4ccb8e64c25393159b6b)
# MAGIC 1. [Get started with SQL](https://datasentics.notion.site/Get-started-with-SQL-c0938f98ddb14dfdab2fdb6b87b1331a)
# MAGIC 1. [Get started with PySpark](https://www.notion.so/datasentics/Get-started-with-PySpark-d3a7f94347284d9a89f37cfaf8b345bc?pvs=4)
# MAGIC 1. [FAQ](https://datasentics.notion.site/FAQ-339dfdaed92e41518f75c4a839d6ca71)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo project overview
# MAGIC 
# MAGIC This demo is a reference project implemented using ODAP Framework containing example generated data from multiple data source, feature engineering code, definition of segments for multiple use cases and orchestration of exports.
# MAGIC 
# MAGIC Navigation:
# MAGIC - [To find examples of feature engineering code, go to the `features` folder]($./features/customer_email)
# MAGIC - [To find examples of segment definition code, go to the `use_cases/{name_of_use_case}/segments` folder]($./use_cases/upsell_uc/segments/customer_account_investment_interest)
# MAGIC - [To find orchestration of features]($./orchestration/features_orchestrator)
# MAGIC - [To find orchestration of exports]($./orchestration/segments_orchestrator)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo data + use cases
# MAGIC 
# MAGIC Demo models an end-2-end use case built on top of 3 datasets
# MAGIC - `card_transactions`
# MAGIC - `web_visits` events
# MAGIC - and static `customer` data.
# MAGIC 
# MAGIC Input data also contains a table called `targets` which holds the targets for classification models.
# MAGIC 
# MAGIC This data is then used to calculate 2 historized feature tables: `product_features` and `simple_features`.
# MAGIC 
# MAGIC Latest snapshot of these features is precalculated into the `features_latest` table.
# MAGIC 
# MAGIC Segments for exporting are then calculated from this `latest` table and logged in the `segments` table for audit purposes. 

# COMMAND ----------

# MAGIC %md
# MAGIC ![demo lineage](https://github.com/DataSentics/odap/raw/main/odap_framework_demo/_demo/_img/demo_lineage.png "Demo lineage")
