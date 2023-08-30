-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Email feature

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Widgets

-- COMMAND ----------

create widget text target default "no target";
create widget text timestamp default "2020-12-12";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Init target store

-- COMMAND ----------

-- MAGIC %run ../init/target_store

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Define metadata

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metadata = {
-- MAGIC     "table": "simple_features",
-- MAGIC     "category": "personal",
-- MAGIC     "features": {
-- MAGIC         "customer_email": {
-- MAGIC             "description": "User's email",
-- MAGIC             "tags": ["email", "sensitive"],
-- MAGIC         }
-- MAGIC     }
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Define DQ checks

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dq_checks = [
-- MAGIC     {
-- MAGIC         "invalid_count(customer_email) = 0": {
-- MAGIC             "valid regex": r"^[a-zA-Z0-9.]+@[a-zA-Z0-9-.]+$"
-- MAGIC         }
-- MAGIC     }
-- MAGIC ]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calculate features

-- COMMAND ----------

select
  customer_id,
  t.timestamp,
  customer_email
from
  ${env:READ_ENV}.odap_offline_sdm_l2.customer join target_store t using (customer_id)
-- union (
--   select
--     123456789 as customer_id,
--     timestamp(getargument("timestamp")) as timestamp,
--     "invalid_email" as customer_email
-- ) -- uncoment for email dq check validation fail
