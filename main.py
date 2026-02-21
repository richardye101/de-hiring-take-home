import argparse
from datetime import datetime
import logging
import os
import threading

from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from pathlib import Path
from queue import Queue

from src.extract import extract_worker
from src.load import load_worker
from src.models import ExtractItem
from src.transform import transform_worker
from src.tracker import CrawlStats, monitor_worker

load_dotenv()

LOG_FILENAME = datetime.now().strftime("run_%Y-%m-%d_%H-%M-%S.log")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
MAX_LOG_SIZE = int(os.environ.get("MAX_LOG_SIZE_MB", 5)) * 1024 * 1024
BACKUP_COUNT = int(os.environ.get("BACKUP_COUNT", 3))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 100))

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
    handlers=[
        # This replaces the standard FileHandler
        RotatingFileHandler(Path("logs") / LOG_FILENAME, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT),
        logging.StreamHandler(),  # Still print to console
    ],
)
logger = logging.getLogger(__name__)

# Create thread-safe queues
# depending on how many links and crawled, the data could be too large to fit entirely into memory
to_extract_q = Queue()
# store extracted data in a to_transform_q, which transformer workers will pop from
to_transform_q = Queue()
# store transformed data in a to_load_q, which the loader worker will pop from
to_load_q = Queue()


# Initialize one instance in main.py
stats = CrawlStats()


def run_pipeline(webpage: str, max_depth: int, extract_workers: int, transform_workers: int):
    root_page = ExtractItem(link=webpage, depth=0)
    to_extract_q.put(root_page)

    stop_monitor = threading.Event()
    monitor_thread = threading.Thread(target=monitor_worker, args=[stats, stop_monitor])
    monitor_thread.start()

    # spin up the workers for extraction
    extractors = []
    for i in range(extract_workers):
        # Pass the queue reference and max_depth to the worker
        t = threading.Thread(
            target=extract_worker, args=[max_depth, to_extract_q, to_transform_q, stats], name=f"Extract_Worker-{i}"
        )
        t.start()
        extractors.append(t)
    # spin up the workers for transformation
    transformers = []
    for i in range(transform_workers):
        # transform each new page as it arrives in the queue
        # move to a loading deque
        t = threading.Thread(
            target=transform_worker, args=[to_transform_q, to_load_q, stats], name=f"Transform_Worker-{i}"
        )
        t.start()
        transformers.append(t)

    # load from loading deque
    load_thread = threading.Thread(target=load_worker, args=[to_load_q, BATCH_SIZE, stats], name="Load_Worker")
    load_thread.start()

    # Blocks until the extract queue is empty
    to_extract_q.join()
    # Send poison pills for each thread
    for i in range(extract_workers):
        to_extract_q.put(None)
    for t in extractors:
        t.join()
    stats.end_extract()

    # Blocks until the transform queue is empty
    to_transform_q.join()
    # Send poison pills for each thread
    for i in range(transform_workers):
        to_transform_q.put(None)
    for t in transformers:
        t.join()

    to_load_q.join()
    to_load_q.put(None)
    load_thread.join()

    stop_monitor.set()
    monitor_thread.join()
    stats.end()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebCrawler")

    parser.add_argument(
        "-w",
        "--webpage",
        type=str,
        default="https://en.wikipedia.org/wiki/Toronto",
        help="The name of the website to crawl.",
    )
    parser.add_argument("-d", "--depth", type=int, default=2, help="The depth of links to parse.")
    parser.add_argument(
        "-e", "--extract_workers", type=int, default=2, help="The number of concurrent workers to use for extraction."
    )
    parser.add_argument(
        "-t",
        "--transform_workers",
        type=int,
        default=2,
        help="The number of concurrent workers to use for transformation.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Set log level to DEBUG.")

    args = parser.parse_args()
    webpage = args.webpage
    max_depth = args.depth
    extract_workers = args.extract_workers
    transform_workers = args.transform_workers

    logger.info(f"Starting pipeline on {webpage=} with {max_depth=}, {extract_workers=}, {transform_workers=}")
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("src.transform").setLevel(logging.DEBUG)
        logger.debug("[Main] Set logging level to DEBUG")

    stats.start()
    run_pipeline(webpage, max_depth, extract_workers, transform_workers)
    data = stats.report()
    # Can only count the total number of links that made it thorugh the pipeline, so we look at the number of links loaded.
    extract_per_min = round(data["extracted"] / data["elapsed_time"] * 60, 2)
    transform_per_min = round(data["transformed"] / data["elapsed_time"] * 60, 2)
    load_per_min = round(data["loaded"] / data["elapsed_time"] * 60, 2)
    msg = (
        f"\rSeconds Elapsed: {data['elapsed_time']} [Extract] {data['extracted']} links, {extract_per_min}/min | [Tranform] {data['transformed']}, {transform_per_min}/min | [Load] {data['loaded']}, {load_per_min}/min",
        f"DLQ: {stats.get_dlq()}",
    )
    logger.info(msg)
