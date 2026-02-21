import pytest
from unittest.mock import MagicMock, patch
from queue import Queue
import requests
from src.extract import extract_links, extract_worker, ExtractItem, RawData


# We mock these globals because they are likely defined at the module level in extract.py
@pytest.fixture(autouse=True)
def mock_globals():
    with (
        patch("src.extract.visited", set()) as _v,
        patch("src.extract.visited_lock", MagicMock()) as _l,
        patch("src.extract.limiter", MagicMock()) as _lim,
    ):
        yield _v, _l, _lim


def test_extract_links_valid_wiki_links():
    q = Queue()
    html = """
    <div id="bodyContent">
        <a href="/wiki/Toronto">Toronto</a>
        <a href="/wiki/Category:Cities">Ignored (colon)</a>
        <a href="https://google.com">Ignored (external)</a>
    </div>
    """

    extract_links("https://parent.com", html, 0, q)

    assert q.qsize() == 1
    item = q.get()
    assert item.link == "https://en.wikipedia.org/wiki/Toronto"
    assert item.depth == 1
    assert item.parent_link == "https://parent.com"


def test_extract_links_no_content():
    q = Queue()
    html = "<div>No bodyContent ID here</div>"

    # Should return early without crashing
    extract_links("parent", html, 0, q)
    assert q.empty()


@patch("src.extract.get_configured_session")
def test_extract_worker_max_depth_reached(mock_session_factory):
    # Setup
    in_q = Queue()
    out_q = Queue()
    stats = MagicMock()

    # Put an item that is already at max depth
    item = ExtractItem(link="http://test.com", parent_link="parent", depth=2)
    in_q.put(item)
    in_q.put(None)  # Shutdown signal

    extract_worker(max_depth=1, input_queue=in_q, output_queue=out_q, stats=stats)

    # Verify: session.get should NEVER have been called because depth > max_depth
    assert mock_session_factory().get.call_count == 0
    assert out_q.empty()


@patch("src.extract.get_configured_session")
def test_extract_worker_successful_extraction(mock_session_factory):
    # Setup Mocks
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '<div id="bodyContent"></div>'
    mock_session.get.return_value = mock_response
    mock_session_factory.return_value = mock_session

    in_q = Queue()
    out_q = Queue()
    stats = MagicMock()

    in_q.put(ExtractItem(link="https://en.wikipedia.org/wiki/Test", parent_link="", depth=0))
    in_q.put(None)

    extract_worker(max_depth=2, input_queue=in_q, output_queue=out_q, stats=stats)

    # Verify
    assert out_q.qsize() == 1
    res = out_q.get()
    assert res.link == "https://en.wikipedia.org/wiki/Test"
    stats.inc_extracted.assert_called_once()


@patch("src.extract.get_configured_session")
def test_extract_worker_handle_request_exception(mock_session_factory):
    # Setup session to throw an error
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.RequestException("Connection Timeout")
    mock_session_factory.return_value = mock_session

    in_q = Queue()
    out_q = Queue()
    stats = MagicMock()

    in_q.put(ExtractItem(link="https://bad-url.com", parent_link="", depth=0))
    in_q.put(None)

    extract_worker(max_depth=1, input_queue=in_q, output_queue=out_q, stats=stats)

    # Verify DLQ (Dead Letter Queue) was called
    stats.dlq_append.assert_called_once()
    assert out_q.empty()  # Nothing should have been loaded
