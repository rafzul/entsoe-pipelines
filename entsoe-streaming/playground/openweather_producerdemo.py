import time
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import datetime as dt_module
import requests

# create producer
load_dotenv("kafka.env")
op_key = os.environ["OPENWEATHERAPI_KEY"]
# create producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def produce_weather(topic, lat, lon):
    op_session = requests.Session()
    while True:
        try:
            # get weather data
            response = op_session.get(
                f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={op_key}"
            ).json()
            # print(response_json)
            # add current timestamp
            response.update(
                {
                    "measured_time": dt_module.datetime.now().isoformat(),
                    "created_time": dt_module.datetime.now().isoformat(),
                }
            )
            print(response)
            # send to kafka
            # producer.send(topic, value=response_json)
        except requests.exceptions.RequestException as e:
            # if self._session_was_closed(e):
            #     op_session.close()
            #     op_session = requests.Session()
            # else:
            pass
        time.sleep(2)


if __name__ == "__main__":
    produce_weather(topic="openweather", lat="-44.34", lon="10.99")
