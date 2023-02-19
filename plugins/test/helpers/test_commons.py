# commons_test.py

from plugins.helpers import commons


# tes slack notifications
def test_message_pass():
    assert commons.send_slack_notifications("test message").status_code == 200


# test fungsi A
