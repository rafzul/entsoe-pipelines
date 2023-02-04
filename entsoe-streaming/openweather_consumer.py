import time
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import datetime as dt_module

# create consumer


class OpenWeatherConsumer:
    def __init__(self, topic, auto_offset, auto_commit, group_id):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_earlier=auto_offset,
            enable_auto_commit=auto_commit,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

    def consume_weather(self):
        while True:
            for message in self.consumer:
                message = message.value
                print(message)
            time.sleep(2)


if __name__ == "__main__":
    ow = OpenWeatherConsumer(topic="openweather", auto_offset="earliest",
                             auto_commit=True, group_id="consumer.group.id.demo.1")
    ow.consume_weather()
