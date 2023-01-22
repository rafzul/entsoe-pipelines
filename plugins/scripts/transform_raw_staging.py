import os
from dotenv import load_dotenv, find_dotenv
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# class TransformTSRawStaging:


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
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl","google.cloud.hadoop.fs.gcs.GoogleHadoopFS",) \
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
    metrics_label = sys.argv[0]
    start = sys.argv[1]
    end = sys.argv[2]
    country_code = sys.argv[4]
    main(metrics_label=metrics_label, start=start, end=end, country_code=country_code)
