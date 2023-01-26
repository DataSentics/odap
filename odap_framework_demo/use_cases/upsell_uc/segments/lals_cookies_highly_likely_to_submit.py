# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../../../init/odap

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

import datetime as dt
import mlflow
import os
from lookalike_modelling.ml_functions import define_lookalikes, predict
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define values that define the lookalike segment

# COMMAND ----------

entity_id_column_name = "id_col"
entity_name = "cookie"
latest_date = "2022-09-30"
model_uri = "runs:/4546ea3d2ba741ebab80dc3433a857fe/Model_test_lookalike"
segment_name = "customers_likely_to_churn"
criterion_choice = "probability_threshold" #probability_threshold or lookalike_count
slider_value = 0.5 #float for probability threshold, int for lookalike count
env = os.environ["READ_ENV"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data

# COMMAND ----------

df_data = spark.table(
    f"odap_digi_features.features_{entity_name}"
).filter(f.col("timestamp") == dt.datetime.strptime(latest_date, "%Y-%m-%d"))

df_to_enrich = (
    spark.table("odap_digi_use_case_segments.segments")
    .select(entity_id_column_name)
    .filter(f.col("segment") == segment_name)
)

df_inference_dataset = df_data.join(
    df_to_enrich,
    on=entity_id_column_name,
    how="anti",
).withColumn("label", f.lit(0))

# COMMAND ----------

# DBTITLE 1,Load model pipeline for lookalike estimation
model = mlflow.spark.load_model(model_uri)
run_id = model_uri.split("/")[1]

#specify path for the feature names logged in your mlflow experiment
features = mlflow.artifacts.load_text(f"dbfs:/databricks/mlflow-tracking/3460828603755368/{run_id}/artifacts/features.txt")

feature_store_features = [
    feature for feature in features if feature not in ["intercept", "intercept_vector"]
]
        
df_predictions = predict(df_inference_dataset, model, "probability_of_lookalike", entity_id_column_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply criterion and filter value

# COMMAND ----------

df_lookalikes = define_lookalikes(slider_value, df_predictions, entity_id_column_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The quality of the lookalike segment

# COMMAND ----------

avg_prob_in_sample = df_predictions.select(f.avg("probability_of_lookalike")).collect()[0][0]
avg_prob_in_lookalike = df_lookalikes.select(f.avg("probability_of_lookalike")).collect()[0][0]

avg_score_lookalike_improvement = round(avg_prob_in_lookalike / avg_prob_in_sample, 3)
maximal_improvement = round(1 / avg_prob_in_sample, 3)

# COMMAND ----------

# DBTITLE 1,Log important information to mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f"/Users/{username}/lookalike_segments")

with mlflow.start_run():
    mlflow.log_param("Segment to extend", segment_name)
    mlflow.log_param("Entity ID column name", entity_id_column_name)
    mlflow.log_param("Entity name", entity_name)
    mlflow.log_param("Used model", model_uri)
    mlflow.log_param("Criterion for segment definition", criterion_choice)
    mlflow.log_param("Value set for lookalikes definition", slider_value)

    mlflow.log_metric("Lookalike better than chance by", avg_score_lookalike_improvement)
    mlflow.log_metric("Maximal possible improvement against chance", maximal_improvement)

# COMMAND ----------

# DBTITLE 1,Union with existing segment
df_original_segment_final = (
    spark.table("odap_digi_use_case_segments.segments")
    .filter(f.col("segment") == segment_name)
    .drop("export_id", "segment")
)

df_lookalikes_final = df_lookalikes.drop("probability_of_lookalike")

df_final = df_lookalikes_final.union(df_original_segment_final)
