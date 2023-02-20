import requests
import json
import traceback
from dotenv import load_dotenv, find_dotenv
import os
from plugins.helpers.exceptions import AlertSlackError


load_dotenv(find_dotenv("local.env"))
SLACK_WEBHOOK_TOKEN = os.environ["SLACK_WEBHOOK_TOKEN"]


# REUSABLE: send message through slack notifications
def send_slack_notifications(message):
    webhook_url = f"https://hooks.slack.com/services/{SLACK_WEBHOOK_TOKEN}"
    slack_message = {"text": message}
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(slack_message),
            headers={"Content-Type": "application/json"},
        )
        print(response.status_code)
        response.raise_for_status()
        print(response.status_code)
    except requests.HTTPError as e:
        print(f"Response status code: {response.status_code}")
        raise AlertSlackError(
            "Request to slack returned an error %s, the response is:\n%s"
            % (response.status_code, response.text)
        )
    return response


if __name__ == "__main__":
    send_slack_notifications("test message")
