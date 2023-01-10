# Databricks notebook source
# MAGIC %md ## Initial phase

# COMMAND ----------

# MAGIC %md #### Imports & Settings

# COMMAND ----------

import datetime as dt
from hyperopt import hp
from hyperopt.pyll import scope
import json
import math
import mlflow
import mlflow.spark as mlflow_spark
from mlflow.tracking.client import MlflowClient
import plotly.express as px
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    LogisticRegression,
    GBTClassifier,
    DecisionTreeClassifier,
    RandomForestClassifier,
    MultilayerPerceptronClassifier,
    LinearSVC,
    NaiveBayes,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, Normalizer, StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler, StringIndexer, OneHotEncoder
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.util import MLUtils
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t
from lookalike_models.ml_functions import lift_curve_colname_specified, ith, process_multiple_segments_input, compute_lift_train_test, lift_curve, generate_dummy_data
import shap

# COMMAND ----------

# MAGIC %md ####Functions

# COMMAND ----------

def get_date_parts(date_string):
    return [int(date_part) for date_part in date_string.split("-")]

# COMMAND ----------

# MAGIC %md ####Widgets

# COMMAND ----------

dbutils.widgets.dropdown("1. use_dummy_data", "No", ["Yes", "No"])
dbutils.widgets.text("2. latest_date", "2022-09-30")
dbutils.widgets.text("3. entity_name", "cookie")
dbutils.widgets.text("4. entity_id_column_name", "id_col")
dbutils.widgets.text("5. segment_name", "cookies_highly_likely_to_submit_lead")
dbutils.widgets.dropdown("6. downsample",  "Yes", ["Yes", "No"])
dbutils.widgets.text("7. target_ratio", "0.125")
dbutils.widgets.dropdown("8. normalize", "NO NORMALIZER", ["NO NORMALIZER", "Normalizer", "StandardScaler", "MinMaxScaler"])
dbutils.widgets.text("9. model_name", "RandomForest")

# COMMAND ----------

# MAGIC %md #### Load data
# MAGIC Need to specify:
# MAGIC * data to train the lookalike model on
# MAGIC * data with the segment that should serve as a target

# COMMAND ----------

# DBTITLE 1,Define key datasets
if dbutils.widgets.get("1. use_dummy_data") == "Yes":
    df_model_dataset = generate_dummy_data()
else:
    latest_year, latest_month, latest_day = get_date_parts(
        dbutils.widgets.get("2. latest_date")
    )
    
    df_data = spark.table(
        f"odap_digi_features.features_{dbutils.widgets.get('3. entity_name')}"
    ).filter(f.col("timestamp") == dt.date(latest_year, latest_month, latest_day))

    df_to_enrich = (
        spark.table("odap_digi_use_case_segments.segments")
        .select(dbutils.widgets.get("4. entity_id_column_name"))
        .filter(f.col("segment") == dbutils.widgets.get("5. segment_name"))
    )

    df_model_dataset = (
        df_data.join(
            df_to_enrich, on=dbutils.widgets.get("4. entity_id_column_name"), how="inner"
        )
        .withColumn("label", f.lit(1))
        .union(
            df_data.join(
                df_to_enrich,
                on=dbutils.widgets.get("4. entity_id_column_name"),
                how="anti",
            ).withColumn("label", f.lit(0))
        )
    )

# COMMAND ----------

# DBTITLE 1,Downsampling
if (dbutils.widgets.get("6. downsample") == "Yes"):
    target_ratio = dbutils.widgets.get("7. target_ratio")
    data_target = df_model_dataset.filter(f.col('label') == 1)
    data_non_target = df_model_dataset.filter(f.col('label') == 0)

    data_non_target_count_wanted = (data_target.count() / float(target_ratio)) - data_target.count()

    downsample_fraction = data_non_target_count_wanted / data_non_target.count()

    df_model_dataset = data_target.union(data_non_target.sample(fraction=downsample_fraction, seed=42))

# COMMAND ----------

# MAGIC %md ## Features preprocessing - split, scale, normalize

# COMMAND ----------

# DBTITLE 1,The list of features
features_names = [feature for feature in df_model_dataset.columns if feature not in [dbutils.widgets.get("4. entity_id_column_name"), "timestamp"]]

# COMMAND ----------

# DBTITLE 1,Split data
df_train, df_test = df_model_dataset.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# DBTITLE 1,Specifying column types
# define which columns we consider as categorical/numerical
skip_cols = [dbutils.widgets.get("4. entity_id_column_name"), "timestamp", "label"]
cat_cols = [f.name for f in df_model_dataset.schema.fields if isinstance(f.dataType, t.StringType) and f.name not in skip_cols]
num_cols = [f.name for f in df_model_dataset.schema.fields if isinstance(f.dataType, (t.IntegerType, t.DoubleType, t.FloatType, t.DecimalType, t.LongType)) and f.name not in skip_cols]

# COMMAND ----------

# DBTITLE 1,String Indexer, OneHotEncoder, Assembler
stages = [] # stages in our Pipeline
for categoricalCol in cat_cols:
    ### StringIndexer could be applied only on string columns -> ensure this
    df_train = df_train.withColumn(categoricalCol, f.col(categoricalCol).cast('string'))
    df_test = df_test.withColumn(categoricalCol, f.col(categoricalCol).cast('string'))

    ### Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol+"Index", handleInvalid="skip") # skip, error

    ### Use OneHotEncoder to convert categorical variables into binary SparseVectors
    encoder = OneHotEncoder(inputCol=categoricalCol+"Index", outputCol=categoricalCol+"_classVec")

    ### Add stages.  These are not run here, but will be run all at once later on.
    stages += [stringIndexer, encoder]

### vectorAssembler  
assemblerInputs = num_cols + [c + "_classVec" for c in cat_cols]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

stages = stages + [assembler]

# COMMAND ----------

# DBTITLE 1,Normalization
if dbutils.widgets.get("8. normalize") != "NO NORMALIZER":
    scaler_norm = Normalizer(inputCol="features_raw", outputCol="features", p=1.0)
    scaler_stan = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
    scaler_mm = MinMaxScaler(inputCol="features_raw", outputCol="features")
    
    df_train = df_train.withColumnRenamed("features", "features_raw")
    df_test = df_test.withColumnRenamed("features", "features_raw")
    
    if dbutils.widgets.get("8. normalizer") == "Normalizer":
        df_train = scaler_norm.transform(df_train).drop("features_raw")
        df_test = scaler_norm.transform(df_test).drop("features_raw")
        scaler = scaler_norm
    elif dbutils.widgets.get("8. normalizer") == "MinMaxScaler":
        scaler_stan = scaler_stan.fit(df_train)
        df_train = scaler_stan.transform(df_train).drop("features_raw")
        df_test = scaler_stan.transform(df_test).drop("features_raw")
        scaler = scaler_stan
    else:
        scaler_mm = scaler_mm.fit(df_train)
        df_train = scaler_mm.transform(df_train).drop("features_raw")
        df_test = scaler_mm.transform(df_test).drop("features_raw")
        scaler = scaler_mm
    
    stages += [scaler]

else:
    scaler = None    

# COMMAND ----------

# DBTITLE 1,Feature selection
selector = ChiSqSelector(numTopFeatures=12, featuresCol="features",
                        outputCol="selectedFeatures", labelCol="label")

stages = stages + [selector]

# COMMAND ----------

# MAGIC %md ## Model estimation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Target distribution train

# COMMAND ----------

n_rows = df_train.count()

display(df_train.groupBy('label').count().withColumn('share', f.col('count')/n_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the mlflow experiment

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f'/Users/{username}/lookalike_models')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model types

# COMMAND ----------

MODEL_TYPES = [
        "LogisticRegression",
        "SVM",
        "NaiveBayes",
        "XGBoost",
        "DecisionTree",
        "RandomForest",
        "MLP",
]

CLASSIFIERS = {
    "LogisticRegression": LogisticRegression(),
    "XGBoost": GBTClassifier(maxIter=10),
    "DecisionTree": DecisionTreeClassifier(),
    "RandomForest": RandomForestClassifier(),
    "SVM": LinearSVC(),
    "NaiveBayes": NaiveBayes(modelType="multinomial"),
    "MLP": MultilayerPerceptronClassifier(layers=[5000, 250, 250, 2]),
}

# COMMAND ----------

# Assign the model type from the widget
model_name = dbutils.widgets.get("9. model_name")

# COMMAND ----------

# Train and log the model
with mlflow.start_run():
    
    # save basic information
    mlflow.log_param("Model_name", "Test_model")
    mlflow.log_param("Model_type", model_name)
    
    # save features names
    features_names = assembler.getInputCols()
    with open('/tmp/features.txt', 'w') as out_file:
        out_file.write(",".join(features_names))
    mlflow.log_artifact('/tmp/features.txt')

    # choose classifier
    classifier = CLASSIFIERS[model_name].setFeaturesCol("selectedFeatures").setLabelCol("label")

    # add chosen classifier to stages of the pipeline
    spark_pipeline = Pipeline(stages=stages + [classifier])

    #fit the pipeline
    model_pipeline = spark_pipeline.fit(df_train)
        
    # final model pipeline
    model_pipeline_all = PipelineModel(stages = [model_pipeline])

    m_info = mlflow_spark.log_model(spark_model=model_pipeline_all, artifact_path="Model_test_lookalike")

    # predictions
    prediction_df = model_pipeline_all.transform(df_test)
    prediction_df_train = model_pipeline_all.transform(df_train)
    
    score_df = prediction_df.withColumn(
        "score1", ith("probability", f.lit(0))
    ).withColumn("score2", ith("probability", f.lit(1)))
    
    score_df_train = prediction_df_train.withColumn(
        "score1", ith("probability", f.lit(0))
    ).withColumn("score2", ith("probability", f.lit(1)))
    
    
    # lift10 test
    lift10 = lift_curve_colname_specified(
        prediction_df, "label", 10, "probability"
    )

    lift10.toPandas().to_csv("/tmp/lift10.csv")
    mlflow.log_artifact("/tmp/lift10.csv")
    lift10_number = (
        lift10.filter(f.col("bucket") == 1).select("cum_lift").collect()[0][0]
    )
    mlflow.log_param("Lift_10", lift10_number)
    

    # lift10 train
    lift10_train = lift_curve_colname_specified(
        prediction_df_train, "label", 10, "probability"
    )
    lift10_train.toPandas().to_csv('/tmp/lift10_train.csv')
    mlflow.log_artifact('/tmp/lift10_train.csv')
    lift10_train_number = lift10_train.filter(f.col('bucket') == 1).select('cum_lift').collect()[0][0]
    mlflow.log_param('Lift_10_train', lift10_train_number)
    

    # log the lift 100
    lift100 = lift_curve_colname_specified(
        prediction_df, "label", 100, "probability"
    )
    lift100.toPandas().to_csv("/tmp/lift100.csv")
    mlflow.log_artifact("/tmp/lift100.csv")

    lift100_number = (
        lift100.filter(f.col("bucket") == 1).select("cum_lift").collect()[0][0]
    )
    mlflow.log_param("Lift_100", lift100_number)

# COMMAND ----------

# MAGIC %md #### Model testing
# MAGIC Confusion matrix with threshold specification and then distribution of probabilities

# COMMAND ----------

prob_th=0.5

# COMMAND ----------

try:
    df_train_model_testing = (score_df_train
                .select('label', f.when(f.col("score2") >= prob_th, 1).otherwise(0).alias("TARGET_PRED"))
                .groupBy('label', "TARGET_PRED")
                .count()
                .sort(f.desc('count'))
                )

    print('train')
    display(df_train)

    df_test_model_testing = (score_df
                .select('label', f.when(f.col("score2") >= prob_th, 1).otherwise(0).alias("TARGET_PRED"))
                .groupBy('label', "TARGET_PRED")
                .count()
                .sort(f.desc('count'))
                )

    print('test')
    display(df_test_model_testing)

    print('accuracy test')
    n_label_1 = df_test_model_testing.filter(f.col('label') == 1).groupBy().agg(f.sum('count').alias('count')).select('count').collect()[0][0]
    n_pred_1 = df_test_model_testing.filter((f.col('label') == 1) & (f.col('TARGET_PRED') == 1)).groupBy().agg(f.sum('count').alias('count')).select('count').collect()[0][0]
    print(n_pred_1/n_label_1)
except BaseException as e:
    print(f"ERROR: preiction_ov_1: {e}")

# COMMAND ----------

try:
    n_train = score_df_train.count()

    df_train_model_testing = (score_df_train
                    .withColumn('pred', f.round('score2', 2))
                    .groupBy("pred")
                    .count()
                    .sort(f.desc("pred"))
                    .withColumn('SHARE', f.col('count')/n_train)
                    )

    print('train')
    display(df_train)

    n_test = score_df.count()

    df_test_model_testing = (score_df
                    .withColumn('pred', f.round('score2', 2))
                    .groupBy("pred")
                    .count()
                    .sort(f.desc("pred"))
                    .withColumn('SHARE', f.col('count')/n_test)
                    )

    print('test')
    display(df_test_model_testing)
except BaseException as e:
    print(f"ERROR: preiction_ov_2: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Get lift

# COMMAND ----------

try:
    display(compute_lift_train_test(score_df_train, score_df, "label", "features").withColumnRenamed('lift_train', 'Lift Train').withColumnRenamed('lift_test', 'Lift Test'))
except BaseException as e:
    print(f"ERROR: lift: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Feature importances

# COMMAND ----------

try:
    model_pipeline_all_fi = model_pipeline_all.stages[-1].stages[-1]
                    
    feature_importances = list(model_pipeline_all_fi.featureImportances.toArray())
    feature_importances_with_names = []

    for feature_name, importance in zip(features_names, feature_importances):
        feature_importances_with_names.append((feature_name, float(importance)))

    feature_importances = spark.createDataFrame(feature_importances_with_names, schema="`feature` STRING, `importance` FLOAT") 
    feature_importances = feature_importances.orderBy("importance")

    importance_min = feature_importances.select('importance').groupBy().agg(f.min('importance')).collect()[0][0]
    importance_max = feature_importances.select('importance').groupBy().agg(f.max('importance')).collect()[0][0]

    if importance_min < 0:
        color_schema = "greenred"
    else:
        color_schema = "rdylgn"
    
    feature_importances = feature_importances.withColumn('feature',  f.regexp_replace('feature', '_', ' '))
    feature_importances = feature_importances.filter(f.col('importance') > 0)
    
    fig = px.bar(feature_importances.orderBy("importance", ascending=False).limit(30).orderBy("importance").toPandas(), x="feature", y="importance",
                        hover_data=["feature", "importance"], color="importance",
                        color_continuous_scale=color_schema,
                        height=700)
    fig.show()
    
    # top 15
    fig = px.bar(feature_importances.orderBy("importance").limit(15).toPandas(), x="feature", y="importance",
                    hover_data=["feature", "importance"], color="importance",
                    color_continuous_scale=color_schema, range_color=[importance_min, importance_max],
                    height=700,)
    fig.show()
    
    # min 15
    fig = px.bar(feature_importances.orderBy("importance", ascending=False).limit(15).orderBy("importance").toPandas(), x="feature", y="importance",
                    hover_data=["feature", "importance"], color="importance",
                    color_continuous_scale=color_schema, range_color=[importance_min, importance_max],
                    height=700, )
    fig.show()
except BaseException as e:
    print(f"ERROR: features importances not available: {e}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Feature values

# COMMAND ----------

df = score_df_train
prob_th = 0.5

try:
    df = (df
            .withColumn("xs", vector_to_array("features"))
            .select(["label", "score2"] + [f.col("xs")[i].alias(name) for i, name in enumerate(features_names)])
            .withColumn("TARGET_PRED", f.when(f.col("score2") >= prob_th, 1).otherwise(0))
            )
    
    df_target = (df
                    .groupBy("label")
                    .agg(
                        f.count(f.lit(1)).alias('count'),
                        *[f.mean(f.col(c)).alias(c) for c in features_names]
                    )
                    .sort('label')
                )
    
    df_pred = (df
                .groupBy("TARGET_PRED")
                .agg(
                    f.count(f.lit(1)).alias('count'),
                    *[f.mean(f.col(c)).alias(c) for c in features_names]
                )
                .sort("TARGET_PRED")
                )
    
    display(df_target)
    display(df_pred)
    
except BaseException as e:
    
        print(f'ERROR: {e}')

# COMMAND ----------

df = score_df
prob_th = 0.5

try:
    df = (df
            .withColumn("xs", vector_to_array("features"))
            .select(["label", "score2"] + [f.col("xs")[i].alias(name) for i, name in enumerate(features_names)])
            .withColumn("TARGET_PRED", f.when(f.col("score2") >= prob_th, 1).otherwise(0))
            )
    
    df_target = (df
                    .groupBy("label")
                    .agg(
                        f.count(f.lit(1)).alias('count'),
                        *[f.mean(f.col(c)).alias(c) for c in features_names]
                    )
                    .sort('label')
                )
    
    df_pred = (df
                .groupBy("TARGET_PRED")
                .agg(
                    f.count(f.lit(1)).alias('count'),
                    *[f.mean(f.col(c)).alias(c) for c in features_names]
                )
                .sort("TARGET_PRED")
                )
    
    display(df_target)
    display(df_pred)
    
except BaseException as e:
        print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Shapley values

# COMMAND ----------

df = score_df_train

try:
    df = (df
            .withColumn("xs", vector_to_array("features"))
            .select(["label"] + [f.col("xs")[i].alias(name) for i, name in enumerate(features_names)])
            )
    
    X = df.select(features_names).toPandas()
    explainer = shap.TreeExplainer(model_pipeline_all.stages[-1].stages[-1])
    shap_values = explainer.shap_values(X, check_additivity=False)
    
    shap.summary_plot(shap_values, X, plot_size=[30,20])
    shap.summary_plot(shap_values, X, plot_type='violin', plot_size=[30,20])
    shap.summary_plot(shap_values, X, plot_type="bar")
    
except BaseException as e:
        print(f'ERROR: Shapley values not available: {e}')
