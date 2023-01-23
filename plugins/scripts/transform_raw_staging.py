import os
from dotenv import load_dotenv, find_dotenv
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# class TransformTSRawStaging:


def clean_columns_n_casttypes(df, parent_column_name):
    # cleaning the dots, changed it into namespaces, casting new columns names
    df_schema = df.select(parent_column_name).dtypes[0][1]
    replacements = [("\.", "_"), ("[@#]", "")]
    for old, new in replacements:
        df_schema = re.sub(old, new, df_schema)
    # casting the DF with the cleaned schema (must be done first before column name got changed)
    df = df.withColumn(
        parent_column_name, F.col(parent_column_name).cast(df_schema)
    ).select(f"{parent_column_name}.*")
    # #casting the DF with correct datatype schema, selecting the column inside the big parent column name
    # df = df.withColumn(parent_column_name, F.col(parent_column_name).cast(raw_schema))
    return df


# define flatten struct function
def flatten_struct(nested_struct_df):
    flat_cols = [c[0] for c in nested_struct_df.dtypes if c[1][:6] != "struct"]
    nested_struct_cols = [c[0] for c in nested_struct_df.dtypes if c[1][:6] == "struct"]
    flat_df = nested_struct_df.select(
        flat_cols
        + [
            F.col(f"{nc}.{c}").alias(f"{nc}_{c}")
            for nc in nested_struct_cols
            for c in nested_struct_df.select(f"{nc}.*").columns
        ]
    )
    return flat_df




def main(metrics_label, start, end, country_code):
    # load env variables
    load_dotenv("/opt/airflow/.env", verbose=True)

    # setting up GCP variables
    gcs_bucket = os.environ.get("GCP_GCS_BUCKET")
    service_account_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    start_label = start
    end_label = end
    landing_filename = (
        f"{metrics_label}__{country_code}__{start_label}__{end_label}.json"
    )
    print(landing_filename)

    # setting up GCP variables
    gcs_bucket = os.environ.get("GCP_GCS_BUCKET")
    SPARK_HOME = os.environ["SPARK_HOME"]

    # fmt: off
    spark = SparkSession.builder \
        .appName("gcp_playground") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars", f"{SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar, {SPARK_HOME}/jars/spark-bigquery-with-dependencies_2.13-0.27.1.jar") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",service_account_file) \
        .getOrCreate()
    # fmt: on

    path = f"gs://{gcs_bucket}/{landing_filename}"
    try:
        df_spark = (
            spark.read.format("json")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(path)
        )
    except Exception as e:
        print(f"Error, ada {e}")
        pass

    # upload to staging
    # fmt: off
    df_spark.write.format("bigquery").option("project", "rafzul-analytics-1009") \
    .option("temporaryGcsBucket", "entsoe_temp_1009") \
    .mode("append") \
    .save("rafzul-analytics-1009.entsoe_playground.TEST_total_generation_staging")
    # fmt: on


if __name__ == "__main__":
    # setting up variables for spark applications
    metrics_label = sys.argv[1]
    start = sys.argv[2]
    end = sys.argv[3]
    country_code = sys.argv[4]
    main(metrics_label=metrics_label, start=start, end=end, country_code=country_code)
