from src.utils import write_to_slack


def test_write_to_slack_errors(mocker):
    errors = {"odds": "IndexError", "boxscores": "NoGameDateError"}

    mocker.patch("src.utils.requests.post").return_value.status_code = 200
    response = write_to_slack(errors)

    assert response == 200


def test_write_to_slack_no_errors():
    errors = {}

    response = write_to_slack(errors)

    assert response is None
