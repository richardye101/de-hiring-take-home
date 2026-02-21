import pytest
from queue import Queue
from unittest.mock import MagicMock, patch
from sqlmodel import Session, create_engine, select
from datetime import datetime

from src.load import load_worker, init_db
from src.models import TransformedData


@pytest.fixture
def memory_engine():
    """Provides a clean in-memory database engine for each test."""
    engine = create_engine("sqlite:///:memory:")
    init_db(engine)
    return engine


@pytest.fixture
def mock_stats():
    return MagicMock()


### --- Tests for Database Loading --- ###


def test_init_db(memory_engine):
    # If init_db worked, we should be able to query the table without error
    with Session(memory_engine) as session:
        # Just checking if the table exists by attempting a select
        results = session.exec(select(TransformedData)).all()
        assert results == []


@patch("src.load.sqlite_url", "sqlite:///:memory:")
def test_load_worker_batch_flush(mock_stats):
    input_q = Queue()
    batch_size = 2

    # Create test data
    data1 = TransformedData(
        link="https://test1.com",
        parent_link="https://test1parent.com",
        title="Title 1",
        content="C1",
        num_links=0,
        num_h2=0,
        num_refs=0,
        word_count=2,
        scraped_at=datetime.now(),
        modified_at_utc=datetime.now(),
    )
    data2 = TransformedData(
        link="https://test2.com",
        parent_link="https://test2parent.com",
        title="Title 2",
        content="C2",
        num_links=0,
        num_h2=0,
        num_refs=0,
        word_count=2,
        scraped_at=datetime.now(),
        modified_at_utc=datetime.now(),
    )

    # 1. Put items in queue (exactly the batch size)
    input_q.put(data1)
    input_q.put(data2)
    input_q.put(None)  # Poison pill

    # 2. Run the worker
    # Note: load_worker creates its own engine based on sqlite_url
    load_worker(input_q, batch_size, mock_stats)

    # 3. Verify Stats
    # inc_loaded should be called with 2 (since batch size was hit)
    # and again at the end if there were leftovers (0 in this case)
    assert mock_stats.inc_loaded.called


@patch("src.load.sqlite_url", "sqlite:///:memory:")
def test_load_worker_poison_pill_flushes_leftovers(mock_stats):
    input_q = Queue()
    batch_size = 10  # Batch size is large

    data1 = TransformedData(
        link="https://leftover.com",
        parent_link="https://leftoverparent.com",
        title="Leftover",
        content="C",
        num_links=0,
        num_h2=0,
        num_refs=0,
        word_count=1,
        scraped_at=datetime.now(),
        modified_at_utc=datetime.now(),
    )

    # Put only 1 item and then shut down
    input_q.put(data1)
    input_q.put(None)

    # Run worker
    load_worker(input_q, batch_size, mock_stats)

    # Check if stats were updated for that 1 leftover item
    mock_stats.inc_loaded.assert_any_call(1)


def test_flush_to_db_merges_data(memory_engine):
    from src.load import _flush_to_db

    data = TransformedData(
        link="https://dup.com",
        parent_link="https://dupparent.com",
        title="Original",
        content="C",
        num_links=0,
        num_h2=0,
        num_refs=0,
        word_count=1,
        scraped_at=datetime.now(),
        modified_at_utc=datetime.now(),
    )

    # 1. First save
    _flush_to_db(memory_engine, [data])

    # 2. Update same link (Primary Key)
    data.title = "Updated"
    _flush_to_db(memory_engine, [data])

    # 3. Verify there is still only 1 record, and it's updated
    with Session(memory_engine) as session:
        results = session.exec(select(TransformedData)).all()
        assert len(results) == 1
        assert results[0].title == "Updated"
