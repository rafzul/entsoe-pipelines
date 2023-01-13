# Import Libraries
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark
from pyspark import SQLContext
from pyspark.sql import SparkSession
import os 
from dotenv import load_dotenv

#load env variables
load_dotenv('../creds/.env', verbose=True, override=True)


# Setup the Configuration
spark = SparkSession.builder.appName("abc").getOrCreate()
# Setup the Schema
schema = StructType([
StructField("User ID", IntegerType(),True),
StructField("Username", StringType(),True),
StructField("Browser", StringType(),True),
StructField("OS", StringType(),True),
])
# Add Data
data = ([(1580, "Barry", "FireFox", "Windows" ),
(5820, "Sam", "MS Edge", "Linux"),
(2340, "Harry", "Vivaldi", "Windows"),
(7860, "Albert", "Chrome", "Windows"),
(1123, "May", "Safari", "macOS")
])

# Setup the Data Frame
user_data_df = spark.createDataFrame(data,schema=schema)
print(user_data_df.head(5))