import requests
import json
import traceback
from dotenv import load_dotenv, find_dotenv
import os
from plugins.helpers.exceptions import AlertsSlackError


load_dotenv(find_dotenv("local.env"))
SLACK_WEBHOOK_TOKEN = os.environ["SLACK_WEBHOOK_TOKEN"]


# REUSABLE: send message through slack notifications
def send_slack_notifications(message):
    webhook_url = f"https://hooks.slack.com/services/{SLACK_WEBHOOK_TOKEN}"
    slack_message = {"text": message}

    response = requests.post(
        webhook_url,
        data=json.dumps(slack_message),
        headers={"Content-Type": "application/json"},
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise AlertsSlackError(
            "Request to slack returned an error %s, the response is:\n%s"
            % (response.status_code, response.text)
        )
    return response


if __name__ == "__main__":
    send_slack_notifications("test message")
