import datetime as dt
import ipywidgets as widgets
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.ml.functions import vector_to_array
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.window import Window
import random

def return_slider(criterion):
    if (criterion == "probability_threshold"):
        slider = widgets.FloatSlider(max=1.0, min=0.0,  step=0.01, value=0.5)
    else:
        slider = widgets.IntSlider(min=100, max=100000, step=100, value=500)
    return slider

def define_lookalikes(value, df_predictions, entity_id_column_name):
    if (isinstance(value, float)):
        df_lookalikes = (
            df_predictions.sort("probability_of_lookalike", ascending=False)
            .filter(
                F.col("probability_of_lookalike") >= value
            )
            .select(entity_id_column_name, "probability_of_lookalike")
        )
        report_string = f"Computation completed. Number of lookalikes to add based on {value} probability threshold: {df_lookalikes.count()}"
    else:
        df_lookalikes = (
            df_predictions.sort("probability_of_lookalike", ascending=False)
            .limit(value)
            .select(entity_id_column_name, "probability_of_lookalike")
        )

        lowest_prob = round(df_lookalikes.select(F.min("probability_of_lookalike")).collect()[0][0], 4)

        report_string = f"Computation completed. The lowest probability of the lookalike subject admitted: {lowest_prob}"
    
    print(report_string)

    return df_lookalikes

def predict(features: DataFrame, model: PipelineModel, prediction_output_col_name: str, entity_id_column_name: str):
    predictions_df = model.transform(features)

    prediction_col = F.round(F.element_at(vector_to_array(F.col("probability")), 2).cast("float"), 3) if "probability" in predictions_df.columns else F.round("prediction", 3)

    return predictions_df.select(
        entity_id_column_name,
        "timestamp",
        prediction_col.alias(prediction_output_col_name),
    )

def generate_dummy_data():
    
    spark = SparkSession.builder.appName('generate_dummy_data').getOrCreate()
    
    timestamp = dt.datetime(2022, 9, 30)

    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("timestamp", T.TimestampType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("gender", T.IntegerType(), True),
        ]
    )

    df_data = spark.createDataFrame(
        [
        (i, timestamp, random.randint(20, 60), i%2) for i in range(100)
        ], 
        schema
    )

    schema_segments = T.StructType(
        [
            T.StructField("export_id", T.StringType(), True),
            T.StructField("segment", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
        ]
    )

    df_to_enrich = spark.createDataFrame(
        [
        ("xesfoij", "test_segment", i*2) for i in range(30)
        ],
        schema_segments
    ).select("customer_id")

    df_model_dataset = df_data.join(df_to_enrich, on="customer_id", how="inner").withColumn("label", F.lit(1)).union(
                            df_data.join(df_to_enrich, on="customer_id", how="anti").withColumn("label", F.lit(0)))
    
    return df_model_dataset

# LIFT
def lift_curve(predictions, target, bin_count):
    vectorElement = udf(lambda v: float(v[1]))
    lift_df = (
        predictions.select(
            vectorElement("category_affinity").cast("float").alias("category_affinity"),
            target,
        )
        .withColumn(
            "rank", ntile(bin_count).over(Window.orderBy(desc("category_affinity")))
        )
        .select("category_affinity", "rank", target)
        .groupBy("rank")
        .agg(
            count(target).alias("bucket_row_number"),
            sum(target).alias("bucket_lead_number"),
            avg("category_affinity").alias("avg_model_lead_probability"),
        )
        .withColumn(
            "cum_avg_leads",
            avg("bucket_lead_number").over(
                Window.orderBy("rank").rangeBetween(Window.unboundedPreceding, 0)
            ),
        )
    )

    avg_lead_rate = (
        lift_df.filter(col("rank") == bin_count)
        .select("cum_avg_leads")
        .collect()[0]
        .cum_avg_leads
    )  # cislo = cum_avg_leads 10. decilu napr(317.2)

    cum_lift_df = lift_df.withColumn(
        "cum_lift", col("cum_avg_leads").cast("float") / avg_lead_rate
    ).selectExpr(
        "rank as bucket",
        "bucket_row_number",
        "bucket_lead_number",
        "avg_model_lead_probability",
        "cum_avg_leads",
        "cum_lift",
    )
    return cum_lift_df

def lift_curve_colname_specified(predictions, target, bin_count, colname):
    vectorElement = udf(lambda v: float(v[1]))
    lift_df = (
        predictions.select(vectorElement(colname).cast("float").alias(colname), target)
        .withColumn("rank", F.ntile(bin_count).over(Window.orderBy(F.desc(colname))))
        .select(colname, "rank", target)
        .groupBy("rank")
        .agg(
            F.count(target).alias("bucket_row_number"),
            F.sum(target).alias("bucket_lead_number"),
            F.avg(colname).alias("avg_model_lead_probability"),
        )
        .withColumn(
            "cum_avg_leads",
            F.avg("bucket_lead_number").over(
                Window.orderBy("rank").rangeBetween(Window.unboundedPreceding, 0)
            ),
        )
    )

    avg_lead_rate = (
        lift_df.filter(F.col("rank") == bin_count)
        .select("cum_avg_leads")
        .collect()[0]
        .cum_avg_leads
    )  # cislo = cum_avg_leads 10. decilu napr(317.2)

    cum_lift_df = lift_df.withColumn(
        "cum_lift", F.col("cum_avg_leads").cast("float") / avg_lead_rate
    ).selectExpr(
        "rank as bucket",
        "bucket_row_number",
        "bucket_lead_number",
        "avg_model_lead_probability",
        "cum_avg_leads",
        "cum_lift",
    )
    return cum_lift_df

# split score
@F.udf(returnType=T.DoubleType())
def ith(v, i):
    try:
        return float(v[i])
    except ValueError:
        return None

def process_multiple_segments_input(t):
    t = t.replace(" ", "")
    
    t_list = list(t.split(","))
        
    t_table_name = t.replace(',', '_')
    
    return {
        'converted_list': t_list,
        'table_name_suffix': t_table_name,
        'db_name': t,
    }

def compute_lift_train_test(predictions_train, predictions_test, label_column, colname):
    lift_train = (
    lift_curve_colname_specified(predictions_train, label_column, 10, colname)
    .select("bucket", "cum_lift")
    .withColumnRenamed("cum_lift", "lift_train")
  )

    lift_test = (
    lift_curve_colname_specified(predictions_test, label_column, 10, colname)
    .select("bucket", "cum_lift")
    .withColumnRenamed("cum_lift", "lift_test")
  )

    return lift_train.join(lift_test, on="bucket")
