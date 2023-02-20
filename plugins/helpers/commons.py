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
    session = requests.Session()
    try:
        response = session.post(
            webhook_url,
            data=json.dumps(slack_message),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AlertSlackError(f"Request to slack returned an HTTP error: {str(e)}")
    return response
