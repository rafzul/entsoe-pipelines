# commons_test.py

from plugins.test.helpers import test_commons


# tes slack notifications
def test_message_pass():
    assert test_commons.send_slack_notifications("test message") == True


# test fungsi A
