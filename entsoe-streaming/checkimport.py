import time
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import datetime as dt_module
import requests

load_dotenv("kafka.env")
op_key = os.environ["OPENWEATHERAPI_KEY"]
print(op_key)
