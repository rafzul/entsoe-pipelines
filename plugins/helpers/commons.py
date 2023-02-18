import requests
import json
import traceback
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv("local.env", verbose=True))
SLACK_WEBHOOK_TOKEN = os.environ["SLACK_WEBHOOK_TOKEN"]

webhook_url = f"https://hooks.slack.com/services/{SLACK_WEBHOOK_TOKEN}"


# REUSABLE: send message through slack notifications
def send_slack_notifications(message: str):
    try:
        slack_message = {"message": message}
        response = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            retries=3,
            data=json.dumps(slack_message),
        )
    except Exception:
        traceback.print_exc()
    return True
