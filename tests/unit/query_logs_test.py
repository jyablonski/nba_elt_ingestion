from pathlib import Path

from src.utils import query_logs


def test_query_logs():
    fname = Path(__file__).parent / "../fixtures/test.log"
    logs = query_logs(log_file=fname)

    assert "Reddit Comment Extraction Failed" in logs[0]
    assert "S3 Storage Function Failed" in logs[2]
    assert len(logs) == 3
