import os

from src.utils import query_logs


def test_query_logs():
    fname = os.path.join(os.path.dirname(__file__), "../fixtures/test.log")
    logs = query_logs(log_file=fname)

    assert "Reddit Comment Extraction Failed" in logs[0]
    assert "S3 Storage Function Failed" in logs[2]
    assert len(logs) == 3
