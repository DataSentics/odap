-- Databricks notebook source
CREATE OR REPLACE FUNCTION is_time_window(
    ts TIMESTAMP,
    date_col TIMESTAMP,
    time_window INTERVAL
)
RETURNS BOOLEAN
RETURN date_col > ts - time_window and date_col < ts

-- COMMAND ----------

CREATE OR REPLACE FUNCTION time_windowed_string(
    col STRING,
    ts TIMESTAMP,
    date_col TIMESTAMP,
    time_window INTERVAL
)
RETURNS STRING
RETURN CASE WHEN is_time_window(ts, date_col, time_window) THEN col ELSE NULL END

-- COMMAND ----------

CREATE OR REPLACE FUNCTION time_windowed_long(
    col LONG,
    ts TIMESTAMP,
    date_col TIMESTAMP,
    time_window INTERVAL
)
RETURNS LONG
RETURN CASE WHEN is_time_window(ts, date_col, time_window) THEN col ELSE NULL END

-- COMMAND ----------

CREATE OR REPLACE FUNCTION time_windowed_double(
    col DOUBLE,
    ts TIMESTAMP,
    date_col TIMESTAMP,
    time_window INTERVAL
)
RETURNS DOUBLE
RETURN CASE WHEN is_time_window(ts, date_col, time_window) THEN col ELSE NULL END

-- COMMAND ----------

CREATE OR REPLACE FUNCTION time_windowed_boolean(
    col BOOLEAN,
    ts TIMESTAMP,
    date_col TIMESTAMP,
    time_window INTERVAL
)
RETURNS BOOLEAN
RETURN CASE WHEN is_time_window(ts, date_col, time_window) THEN col ELSE NULL END
