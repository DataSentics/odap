-- Databricks notebook source
-- MAGIC %run ../init/target_store

-- COMMAND ----------

create widget text target default "";
create widget text timestamp default ""

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

-- MAGIC %python
-- MAGIC dq_checks = [
-- MAGIC     {
-- MAGIC         "invalid_count(customer_email) = 0": {
-- MAGIC             "valid regex": r"^[a-zA-Z0-9.]+@[a-zA-Z0-9-.]+$"
-- MAGIC         }
-- MAGIC     }
-- MAGIC ]

-- COMMAND ----------

select
  customer_id,
  t.timestamp,
  customer_email
from
  hive_metastore.odap_offline_sdm_l2.customer join target_store t using (customer_id)
-- union (
--   select
--     123456789 as customer_id,
--     timestamp(getargument("timestamp")) as timestamp,
--     "invalid_email" as customer_email
-- ) -- uncoment for email dq check validation fail
