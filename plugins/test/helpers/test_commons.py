# commons_test.py


import pytest
import requests
from plugins.helpers import commons
from plugins.helpers.exceptions import AlertsSlackError


# tes slack notifications
def test_send_slack_notifications_sucess(mocker):
    mock_post = mocker.patch.object(requests, "post")
    mock_post.return_value.status_code = 200

    result = commons.send_slack_notifications("test message")

    assert result.status_code == 200
    mock_post.assert_called_once()


# test fungsi A
