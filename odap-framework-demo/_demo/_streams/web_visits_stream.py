# Databricks notebook source
import datetime as dt
import time
import random

# COMMAND ----------

clients = [row.customer_id for row in spark.read.table("odap_offline_sdm_l2.customer").limit(1000).collect()]

# COMMAND ----------

urls = [
    "https://acme.com/pujcky",
    "https://acme.com/hypoteky",
    "https://acme.com/investice",
    "https://acme.com/login",
    "https://acme.com/logout",
    "https://acme.com/",
]

# COMMAND ----------

while True:
    (
        spark.createDataFrame(
            [
                [random.choice(clients), dt.datetime.now(), random.choice(urls)]
            ],
            ["customer_id", "visit_timestamp", "url"]
        )
        .write
        .mode("append")
        .saveAsTable("odap_digi_sdm_l2.web_visits_stream")
    )

    time.sleep(random.randint(1, 20))
