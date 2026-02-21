import pytest
from queue import Queue
from datetime import datetime
from unittest.mock import MagicMock
from src.transform import parse_wiki_date, transform_worker
from src.models import RawData, TransformedData

########################
### parse_wiki_date  ###
########################


def test_parse_wiki_date_success():
    text = "This page was last edited on 19 February 2026, at 01:01 (UTC)."
    dt = parse_wiki_date(text)
    assert dt.day == 19
    assert dt.month == 2
    assert dt.year == 2026
    assert dt.hour == 1
    assert dt.minute == 1


def test_parse_wiki_date_invalid_format():
    # Should fallback to datetime.now() without crashing
    text = "Not a date string"
    dt = parse_wiki_date(text)
    assert isinstance(dt, datetime)
    assert dt.date() == datetime.now().date()


########################
### transform_worker ###
########################


@pytest.fixture
def queues():
    return Queue(), Queue()


@pytest.fixture
def mock_stats():
    return MagicMock()


def test_transform_worker_full_flow(queues, mock_stats):
    input_q, output_q = queues

    # 1. Create Mock HTML
    html_content = """
    <html>
        <body>
            <h1 id="firstHeading">Toronto</h1>
            <div class="mw-content-container">
                <h2>History</h2>
                <p>Toronto is a city. <a href="/wiki/Canada">Canada</a></p>
                <table>Ignore this table</table>
                <div class="mw-references-wrap">
                    <ol class="references">
                        <li>Ref 1</li>
                        <li>Ref 2</li>
                    </ol>
                </div>
            </div>
            <div id="footer-info-lastmod">This page was last edited on 10 January 2024, at 12:00 (UTC).</div>
        </body>
    </html>
    """

    raw = RawData(
        link="https://en.wikipedia.org/wiki/Toronto", parent_link="", content=html_content, scraped_at=datetime.now()
    )

    # 2. Feed the queue
    input_q.put(raw)
    input_q.put(None)  # Shutdown signal

    # 3. Run worker
    transform_worker(input_q, output_q, mock_stats)

    # 4. Assertions
    assert output_q.qsize() == 1
    result = output_q.get()

    assert result.title == "Toronto"
    assert result.num_links == 1
    assert result.num_h2 == 1
    assert result.num_refs == 2
    # Table should be decomposed, so "Ignore this table" shouldn't be in word count
    assert "Ignore this table" not in result.content
    assert result.word_count > 0
    assert result.modified_at_utc.year == 2024

    # Verify stats
    mock_stats.inc_transformed.assert_called_once()


def test_transform_worker_missing_elements(queues, mock_stats):
    input_q, output_q = queues

    # Minimal HTML with missing tags
    raw = RawData(
        link="https://test.com",
        parent_link="",
        content="<html><body>No tags here</body></html>",
        scraped_at=datetime.now(),
    )

    input_q.put(raw)
    input_q.put(None)

    transform_worker(input_q, output_q, mock_stats)

    result = output_q.get()
    assert result.title == "Unknown Title"
    assert result.num_refs == 0
    assert result.word_count == 0  # No mw-content-container, so text is empty


def test_transform_worker_exception_handling(queues, mock_stats):
    input_q, output_q = queues

    # Passing something that isn't a RawData object to trigger an exception
    input_q.put("not a raw data object")
    input_q.put(None)

    transform_worker(input_q, output_q, mock_stats)

    # Verify DLQ (Dead Letter Queue) was called due to the error
    mock_stats.dlq_append.assert_called_once()
    assert output_q.empty()
