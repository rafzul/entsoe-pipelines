import time
import json
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import datetime as dt_module
import requests

# create producer


class OpenWeatherProducer:
    def __init__(self, server):
        # load env variables needed
        load_dotenv("kafka.env")
        self.op_key = os.environ["OPENWEATHERAPI_KEY"]
        # create producer
        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def _session_was_closed(self, e):
        # check if session was closed
        exception_types = (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout,
        )
        if isinstance(e, exception_types):
            return True
        else:
            pass

    def produce_weather(self, topic, lat, lon):
        op_session = requests.Session()
        while True:
            try:
                # get weather data api response as dict
                response = op_session.get(
                    f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={self.op_key}"
                ).json()
                # add current timestamp
                response.update(
                    {
                        "measured_time": dt_module.datetime.now().isoformat(),
                        "created_time": dt_module.datetime.now().isoformat(),
                    }
                )
                # send to kafka
                self.producer.send(topic, value=response)
                print("ngirim")
            except requests.exceptions.RequestException as e:
                if self._session_was_closed(e):
                    op_session.close()
                    op_session = requests.Session()
                else:
                    pass
            time.sleep(2)


if __name__ == "__main__":
    ow = OpenWeatherProducer(server=["localhost:9092"])
    ow.produce_weather(topic="openweather", lat="-44.34", lon="10.99")
