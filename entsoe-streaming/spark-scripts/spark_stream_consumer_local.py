import time
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import datetime as dt_module
import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

load_dotenv("~/rafzul/projects/entsoe-pipelines/local.env", verbose=True, override=True)
SPARK_HOME = os.environ["SPARK_HOME"]
# setup sparksession
spark = (
    SparkSession.builder.appName("openweather-streaming")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)



spark.sparkContext.setLogLevel("ERROR")
