import os
import pandas as pd
from dotenv import load_dotenv
import sys
import re

# pyspark modules
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# custom-made modules
from plugins.helpers.parsers import parse_datetimeindex, parse_generation_timeseries

# TAROH DI DAG
start = pd.Timestamp("202101010000", tz="Europe/Berlin")
end = pd.Timestamp("202101010600", tz="Europe/Berlin")


label_data = "total_generation"
start = start.strftime("%Y%m%d%H%M")
end = end.strftime("%Y%m%d%H%M")
country_code = "DE_TENNET"

# TAROH DI FILE CLASSNYA


class EntsoeRawTS:
    def __init__(self):
        # load env variable and setup variable
        # load_dotenv("/opt/airflow/.env", verbose=True)
        load_dotenv("/opt/airflow/.env", verbose=True, override=True)
        SPARK_HOME = os.environ["SPARK_HOME"]
        SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        # setup spark session
        self.spark = (
            SparkSession.builder.appName("gcp_playground")
            .config(
                "spark.jars",
                f"{SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar, {SPARK_HOME}/jars/spark-bigquery-with-dependencies_2.13-0.27.1.jar",
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            )
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                SERVICE_ACCOUNT_FILE,
            )
            .getOrCreate()
        )
        # spark method map
        self.method_map = {
            "total_generation": "transform_generation",
        }

    def _base_timeseries(
        self, metrics_label: str, start: str, end: str, country_code: str
    ):
        # initializing variables
        start_label = start
        end_label = end

        # initializing source path
        landing_filename = (
            f"{metrics_label}__{country_code}__{start_label}__{end_label}.json"
        )
        GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
        path = f"gs://{GCS_BUCKET}/{landing_filename}"

        # create dataframe
        try:
            df_spark = (
                self.spark.read.format("json")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm'Z'")
                .load(path)
            )
        except Exception as e:
            raise e

        # deciding if it's timeseries, clean columns & cast types,
        if metrics_label in ["total_generation"]:
            document_column = "GL_MarketDocument"
        df_spark = self._clean_columns_n_casttypes(df_spark, document_column)

        # separate df into ts and non ts df
        df_ts = df_spark.select("TimeSeries")
        df_nonts = df_spark.drop("TimeSeries")

        # initial processing non TS dataframe
        df_nonts = self._flatten_struct(self._flatten_struct(df_nonts))
        # cast the timestamp column to timestamp type
        df_nonts = (
            df_nonts.withColumn(
                "time_Period_timeInterval_end",
                F.to_timestamp("time_Period_timeInterval_end", "yyyy-MM-dd'T'HH:mm'Z'"),
            )
            .withColumn(
                "time_Period_timeInterval_start",
                F.to_timestamp(
                    "time_Period_timeInterval_start", "yyyy-MM-dd'T'HH:mm'Z'"
                ),
            )
            .withColumn(
                "createdDateTime",
                F.to_timestamp("createdDateTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            )
        )

        # initial processing for TS dataframe
        # explode timeseries column into struct, jadiin semua elemen di dalam array TimeSeries jadi satu row
        df_ts = df_ts.withColumn("TimeSeries", F.explode("TimeSeries"))
        df_ts = df_ts.select("TimeSeries.*")
        # flatten nested struct sampe ke dalem, nyisain si period
        df_ts = self._flatten_struct(self._flatten_struct(df_ts))

        return df_ts, df_nonts

    ##
    ## BASE TIMESERIES UTILITIES

    def _clean_columns_n_casttypes(self, df, parent_column_name):
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
    def _flatten_struct(self, nested_struct_df):
        flat_cols = [c[0] for c in nested_struct_df.dtypes if c[1][:6] != "struct"]
        nested_struct_cols = [
            c[0] for c in nested_struct_df.dtypes if c[1][:6] == "struct"
        ]
        flat_df = nested_struct_df.select(
            flat_cols
            + [
                F.col(f"{nc}.{c}").alias(f"{nc}_{c}")
                for nc in nested_struct_cols
                for c in nested_struct_df.select(f"{nc}.*").columns
            ]
        )
        return flat_df

    #
    def _stage_to_bq(self, df_spark, metrics_label):
        GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        GCP_GCS_TEMP_BUCKET = os.environ.get("GCP_GCS_TEMP_BUCKET")
        BQ_DATASET = os.environ.get("BQ_DATASET")
        df_spark.write.format("bigquery").option("project", GCP_PROJECT_ID).option(
            "temporaryGcsBucket", GCP_GCS_TEMP_BUCKET
        ).mode("append").save(
            f"{GCP_PROJECT_ID}.{BQ_DATASET}.TEST_{metrics_label}_staging"
        )

    ##
    ## BASE TIMESERIES UTILITIES END
    ##----------------------------------
    ## TRANSFORM METHOD
    ##

    def transform_generation(
        self,
        metrics_label: str,
        interval_start: str,
        interval_end: str,
        country_code: str,
        period_start: datetime,
        period_end: datetime,
        **params,
    ):
        # get full df of timeseries & non timeseries
        full_df = self._base_timeseries(
            metrics_label, interval_start, interval_end, country_code
        )
        df_ts = full_df[0]
        df_nonts = full_df[1]
        ##processing TS dataframe
        # setting up per_plant & include_eic as placeholder default
        per_plant = False
        include_eic = False

        # setting up initial dfs used biar ga berulang2 manggil
        all_df = parse_datetimeindex(self.spark, df_ts, df_nonts, tz=None)
        # print(all_df[0])
        # print(all_df[1])

        # select transform_generation special column
        periods_col = df_ts.select(F.col("Period_point")).collect()
        psrtype_col = df_ts.select(F.col("MktPSRType_psrType")).collect()
        metric_col = df_ts.select(F.col("inBiddingZone_Domain_mRID_text")).collect()

        # get range len of periods_col
        for entry in range(len(periods_col)):
            ts_data = parse_generation_timeseries(
                self.spark,
                entry,
                periods_col,
                psrtype_col,
                metric_col,
                per_plant=per_plant,
                include_eic=include_eic,
            )
            all_df = all_df.join(ts_data, ["position"], how="inner")
        all_df = all_df.orderBy(F.asc("position")).drop("position")
        # truncating to given period
        all_df = all_df.where(
            f"measured_at >= to_timestamp('{period_start}') AND measured_at < to_timestamp('{period_end}')"
        )

        # stage data to bigquery
        self._stage_to_bq(all_df, metrics_label)
        print("MANTEP!!!!")


def main(
    metrics_label: str,
    interval_start: str,
    interval_end: str,
    country_code: str,
    period_start: datetime,
    period_end: datetime,
    **params,
):
    # create EntsoeRawTS object
    entsoe = EntsoeRawTS()
    # get the method from the method_map dict
    method_name = entsoe.method_map.get(metrics_label, None)
    if method_name:
        method = getattr(entsoe, method_name)
        run_method = method(
            metrics_label,
            interval_start,
            interval_end,
            country_code,
            period_start,
            period_end,
            **params,
        )
    else:
        raise AttributeError(
            "No such method for metrics_label: {}".format(metrics_label)
        )


if __name__ == "__main__":
    # setting up variables for spark applications
    metrics_label = sys.argv[1]
    interval_start = sys.argv[2]
    interval_end = sys.argv[3]
    country_code = sys.argv[4]
    period_start = sys.argv[5]
    period_end = sys.argv[6]
    main(
        metrics_label=metrics_label,
        interval_start=start,
        interval_end=end,
        country_code=country_code,
        period_start=period_start,
        period_end=period_end,
    )
