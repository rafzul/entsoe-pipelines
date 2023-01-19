# coba spark gcs connector
# setup sparksession for entry point - COBA GCS CONNECTOR


# class TransformTSRawStaging:


def main():
    # load env variables
    load_dotenv("/opt/airflow/.env", verbose=True)
    # setting up GCP variables
    gcs_bucket = os.environ.get("GCP_GCS_BUCKET")
    SPARK_HOME = os.environ["SPARK_HOME"]

    # fmt: off
    spark = (
        SparkSession.builder.appName("gcp_playground")
        .config("spark.jars",f"{SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar, {SPARK_HOME}/jars/spark-bigquery-with-dependencies_2.13-0.27.1.jar",)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl","google.cloud.hadoop.fs.gcs.GoogleHadoopFS",)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",service_account_file,)
        .getOrCreate()
    )
    # fmt: on

    try:
        df_spark = (
            spark.read.format("json")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .load(path)
        )
    except Exception as e:
        pass
