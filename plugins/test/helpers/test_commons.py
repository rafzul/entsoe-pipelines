# commons_test.py


import pytest
import requests
from plugins.helpers import commons, exceptions


# tes slack notifications
def test_send_slack_notifications_sucess(mocker):
    mock_post = mocker.patch.object(requests, "post")
    mock_post.return_value.status_code = 200

    result = commons.send_slack_notifications("test message")

    assert result.status_code == 200
    mock_post.assert_called_once()


def test_send_slack_notifications_failure(mocker):
    mock_post = mocker.patch.object(requests, "post")
    mock_post.return_value.status_code = 400
    mock_post.return_value.raise_for_status.side_effect = requests.HTTPError()

    with pytest.raises(exceptions.AlertSlackError):
        commons.send_slack_notifications("test message")

    mock_post.assert_called_once()


# test fungsi A
