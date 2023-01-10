# Databricks notebook source
# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

import datetime as dt
import ipywidgets as widgets
import mlflow
import plotly.express as px
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create widgets

# COMMAND ----------

dbutils.widgets.text("entity_id_column_name", "customer_id")
dbutils.widgets.text("entity_name", "customer")
dbutils.widgets.text("latest_date", "2022-09-30")
dbutils.widgets.text("model_uri", "runs:/1ffc9dd4c3834751b132c70df455a00d/pipeline")
dbutils.widgets.text("segment_name", "customers_likely_to_churn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def define_lookalikes(value, df_predictions):
    if (isinstance(value, float)):
        df_lookalikes = (
            df_predictions.sort("probability_of_lookalike", ascending=False)
            .filter(
                f.col("probability_of_lookalike") >= value
            )
            .select(dbutils.widgets.get("entity_id_column_name"), "probability_of_lookalike")
        )
        report_string = f"Computation completed. Number of lookalikes to add based on {value} probability threshold: {df_lookalikes.count()}"
    else:
        df_lookalikes = (
            df_predictions.sort("probability_of_lookalike", ascending=False)
            .limit(value)
            .select(dbutils.widgets.get("entity_id_column_name"), "probability_of_lookalike")
        )

        lowest_prob = round(df_lookalikes.select(f.min("probability_of_lookalike")).collect()[0][0], 4)

        report_string = f"Computation completed. The lowest probability of the lookalike subject admitted: {lowest_prob}"
    
    print(report_string)

    return df_lookalikes

# COMMAND ----------

def get_date_parts(date_string):
    return [int(date_part) for date_part in date_string.split("-")]

# COMMAND ----------

def predict(features: DataFrame, model: PipelineModel, feature_name: str):
    predictions_df = model.transform(features)

    if "probability" in predictions_df.columns:
        df_output = predictions_df.select(
            dbutils.widgets.get("entity_id_column_name"),
            "timestamp",
            f.round(
                f.element_at(vector_to_array(f.col("probability")), 2).cast("float"), 3
            ).alias(feature_name),
        )
    else:
        df_output = predictions_df.select(
            dbutils.widgets.get("entity_id_column_name"),
            "timestamp",
            f.round("prediction", 3).alias(feature_name),
        )
    return df_output

# COMMAND ----------

def return_slider(criterion):
    if (criterion == "probability_threshold"):
        slider = widgets.FloatSlider(max=1.0, min=0.0,  step=0.01, value=0.5)
    else:
        slider = widgets.IntSlider(min=100, max=100000, step=100, value=500)
    return slider

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data

# COMMAND ----------

latest_year, latest_month, latest_day = get_date_parts(
    dbutils.widgets.get("latest_date")
)

df_data = spark.table(
    f"odap_digi_features.features_{dbutils.widgets.get('entity_name')}"
).filter(f.col("timestamp") == dt.date(latest_year, latest_month, latest_day))

df_to_enrich = (
    spark.table("odap_digi_use_case_segments.segments")
    .select(dbutils.widgets.get("entity_id_column_name"))
    .filter(f.col("segment") == dbutils.widgets.get("segment_name"))
)

df_inference_dataset = df_data.join(
    df_to_enrich,
    on=dbutils.widgets.get("entity_id_column_name"),
    how="anti",
).withColumn("label", f.lit(0))

df_modelling_dataset = df_data.join(
    df_to_enrich,
    on=dbutils.widgets.get("entity_id_column_name"),
    how="semi",
).withColumn("label", f.lit(1)).union(df_inference_dataset)

# COMMAND ----------

# DBTITLE 1,Load model pipeline for lookalike estimation
model = mlflow.spark.load_model(dbutils.widgets.get("model_uri"))
run_id = dbutils.widgets.get("model_uri").split("/")[1]

#specify path for the feature names logged in your mlflow experiment
features = mlflow.artifacts.load_text(f"dbfs:/databricks/mlflow-tracking/21eba1de169f4aabb0c709f2f34475ed/{run_id}/artifacts/features.txt")

feature_store_features = [
    feature for feature in features if feature not in ["intercept", "intercept_vector"]
]
        
df_predictions = predict(df_inference_dataset, model, "probability_of_lookalike")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set criterions for creating the lookalike segment
# MAGIC Choose probability threshold criterion to set the lowest probability for the observation to be admitted to the lookalike segment.  
# MAGIC Choose lookalike_ids_count criterion to set the number of sorted observations (by probability of lookalike) admitted to the the lookalike segment.

# COMMAND ----------

criterion_choice = widgets.RadioButtons(
    options=['probability_threshold', 'lookalike_ids_count'],
    disabled=False,
    description="Filter criterion"
)

criterion_choice

# COMMAND ----------

slider = return_slider(criterion_choice.value)
slider

# COMMAND ----------

df_lookalikes = define_lookalikes(slider.value, df_predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The quality of the lookalike segment

# COMMAND ----------

avg_prob_in_sample = df_predictions.select(f.avg("probability_of_lookalike")).collect()[0][0]
avg_prob_in_lookalike = df_lookalikes.select(f.avg("probability_of_lookalike")).collect()[0][0]

avg_score_lookalike_improvement = round(avg_prob_in_lookalike / avg_prob_in_sample, 3)
maximal_improvement = round(1 / avg_prob_in_sample, 3)

print("The average score of the lookalike segment is " + str(avg_score_lookalike_improvement) + " times higher than the average score when lookalikes are picked randomly.")
print("Maximal possible difference is " + str(maximal_improvement) + " times higher.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize scores
# MAGIC At the end of the section is a chart with scores depicted for each group of the model dataset.   
# MAGIC These groups consist of lookalikes, original segment and other observations not marked as a lookalike.  
# MAGIC You can compare scores for these groups to assess whether chosen lookalikes have similar scores to original segment (representing targets) and whether not chosen observations have lower scores than original segment.

# COMMAND ----------

# DBTITLE 1,Find out which observations belong to particular visualization groups
df_visualization_groups = (
    df_modelling_dataset.join(df_lookalikes, how="anti", on="id_col")
    .withColumn(
        "group", f.when(f.col("label") == 1, "original_segment").otherwise("not_chosen")
    )
    .union(
        df_modelling_dataset.join(
            df_lookalikes.select("id_col"), how="right", on="id_col"
        ).withColumn("group", f.lit("lookalikes"))
    )
    .select("id_col", "group")
)

# COMMAND ----------

# DBTITLE 1,Score the dataset for visualization
df_visualization_scores = predict(
    df_modelling_dataset, model, "probability_of_lookalike"
).join(df_visualization_groups, on="id_col")

# COMMAND ----------

# DBTITLE 1,Find out the sample rate
original_count = df_visualization_scores.count()
sample_rate = 200000 / original_count

# COMMAND ----------

# DBTITLE 1,Create the histogram
fig = px.histogram(df_visualization_scores.sample(sample_rate).toPandas(), x="probability_of_lookalike", color="group", marginal="rug",
                         hover_data=["group"], histnorm='percent'
                         )
fig.show()
