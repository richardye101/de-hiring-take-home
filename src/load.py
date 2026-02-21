import os
import logging

from dotenv import load_dotenv
from queue import Queue
from sqlmodel import SQLModel, Session, create_engine
from typing import List

from src.tracker import CrawlStats
from src.models import TransformedData

load_dotenv()
DB_PATH = os.environ.get("DB_PATH", "crawl.db")

logger = logging.getLogger(__name__)


def init_db(engine):
    """
    Creates tables in the database for all SQLModel in files that have been imported
    """
    SQLModel.metadata.create_all(engine)


sqlite_url = f"sqlite:///{DB_PATH}"


def load_worker(input_queue: Queue[TransformedData], batch_size: int, stats: CrawlStats):
    engine = create_engine(sqlite_url)
    init_db(engine)
    batch: List[TransformedData] = []
    logger.info("Worker started")
    while True:
        try:
            load_count = 0
            result = input_queue.get()

            if result is None:  # Poison Pill
                if batch:
                    load_count = _flush_to_db(engine, batch)
                logger.info("Worker received shutdown signal")
                stats.inc_loaded(load_count)
                break

            # Since 'result' is already a SQLModel object,
            # we can just add it to the batch
            # logger.info(f"Added {result.link} to batch")
            batch.append(result)

            if len(batch) >= batch_size:
                load_count = _flush_to_db(engine, batch)
                batch = []
            stats.inc_loaded(load_count)
        except Exception as e:
            logger.error(f"Error: {e}\tFailed to load: {result}")
            for i in batch:
                stats.dlq_append(("load", i.link, e))
            continue
        finally:
            # logger.info("Calling task_done")
            input_queue.task_done()


def _flush_to_db(engine, batch) -> int:
    with Session(engine) as session:
        for item in batch:
            # 'executemany' equivalent in SQLModel:
            # This handles "Insert or Update" logic via merge
            session.merge(item)
        session.commit()
    logger.debug(f"Batched {len(batch)} records to SQLite.")
    return len(batch)
