import logging
import time

from src.decorators import time_function


def test_time_function_decorator(mock_logging):
    @time_function
    def jacobs_dummy_function():
        time.sleep(1)  # Sleep for 1 second to simulate a time-consuming task

    jacobs_dummy_function()

    assert mock_logging.record_tuples[0][0] == "root"
    assert mock_logging.record_tuples[0][1] == logging.INFO
    assert "jacobs_dummy_function" in mock_logging.record_tuples[0][2]
    assert "seconds" in mock_logging.record_tuples[0][2]

    # pull the actual execution time out, turn it into a float
    # and assert it's >= 1 second(s)
    assert (
        float(
            mock_logging.record_tuples[0][2]
            .split("took")[1]
            .split("seconds")[0]
            .strip()
        )
        >= 1.0
    )
