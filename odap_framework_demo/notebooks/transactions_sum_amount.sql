-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Transactions features

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
-- MAGIC ### Init window functions

-- COMMAND ----------

-- MAGIC %run ../init/window_functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Join target store with source data
-- MAGIC Filter out data later than timestamp for historical calculation

-- COMMAND ----------

create
or replace temporary view card_transactions as (
  select
    *
  from
    ${env:READ_ENV}.odap_offline_sdm_l2.card_transactions
    join target_store using (customer_id)
  where
    process_date <= timestamp(getargument("timestamp"))
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calculate features

-- COMMAND ----------

select
  customer_id,
  timestamp,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "30 days")) as transactions_sum_amount_in_last_30d,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "60 days")) as transactions_sum_amount_in_last_60d,
  sum(time_windowed_double(amount_czk, timestamp, process_date, "90 days")) as transactions_sum_amount_in_last_90d
from
  card_transactions
group by
  customer_id, timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Define metadata

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metadata = {
-- MAGIC     "category": "transactions",
-- MAGIC     "table": "product_features",
-- MAGIC     "features": {
-- MAGIC         "transactions_sum_amount_in_last_{time_window}": {
-- MAGIC             "description": "Total volume of transactions in last {time_window}",
-- MAGIC             "fillna_with": 0,
-- MAGIC         }
-- MAGIC     }
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Define DQ checks

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dq_checks = [
-- MAGIC     "missing_percent(transactions_sum_amount_in_last_30d) < 5%",
-- MAGIC     {
-- MAGIC         "avg(transactions_sum_amount_in_last_30d)": {
-- MAGIC             "fail": "when < 5000",
-- MAGIC         }
-- MAGIC     },
-- MAGIC     "missing_percent(transactions_sum_amount_in_last_60d) < 10%",
-- MAGIC     "missing_percent(transactions_sum_amount_in_last_90d) < 15%",
-- MAGIC ]
