import logging
import os
import threading
import time
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from queue import Queue
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin
from urllib3.util.retry import Retry

from src.models import ExtractItem, RawData
from src.tracker import CrawlStats

load_dotenv()
GBL_RATE_LIMIT = int(os.environ.get("GBL_RATE_LIMIT", "1_000"))


# Global rate limiter
class GlobalRateLimiter:
    def __init__(self, requests_per_second):
        self.delay = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_request_time
            sleep_time = self.delay - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.last_request_time = time.time()


limiter = GlobalRateLimiter(requests_per_second=GBL_RATE_LIMIT)

logger = logging.getLogger(__name__)

# Global visited set for this run
visited = set()
visited_lock = threading.Lock()


def get_configured_session():
    """
    Creates a new session with the retry strategy.
    Connection Management: Each session maintains its own connection pool.
    If one thread's connection hangs or gets corrupted, it won't stall the other threads.
    """
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "Crawler/1.0"})
    return session


def extract_links(parent_link: str, response_text: str, current_depth: int, input_queue: Queue[ExtractItem]):
    """
    shared_queue is a reference to the Queue created in main.py
    """
    soup = BeautifulSoup(response_text, "html.parser")
    content = soup.find(id="bodyContent")

    if not content:
        logger.warning("No content to extract links from")
        return

    for a in content.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/wiki/") and ":" not in href:
            full_url = urljoin("https://en.wikipedia.org", href)
            # Thread-safe check and add
            with visited_lock:
                if full_url not in visited:
                    logger.debug(f"Extracted sub-link: {href}")
                    item = ExtractItem(parent_link=parent_link, link=full_url, depth=current_depth + 1)
                    input_queue.put(item)


def extract_worker(max_depth: int, input_queue: Queue[ExtractItem], output_queue: Queue[RawData], stats: CrawlStats):
    session = get_configured_session()
    logger.info("Worker started")
    while True:
        extract_item = input_queue.get()
        logger.debug(f"Start on {extract_item}")
        if extract_item is None:
            logger.info("Worker received shutdown signal.")
            break

        parent_link = extract_item.parent_link
        url = extract_item.link
        depth = extract_item.depth

        # Page beyond max_depth, ignore and move to next item
        if depth > max_depth:
            logger.debug(f"[{url}] Skipped, depth {depth} is greater than max {max_depth}")
            input_queue.task_done()
            continue

        try:
            # check visited here before the network call to avoid redundant requests
            with visited_lock:
                if url in visited:
                    logger.debug(f"[{url}] Skipped, already visited")
                    continue
                visited.add(url)

            limiter.wait()
            r = session.get(url, timeout=3)
            r.raise_for_status()

            if r.status_code == 200:
                # Passing the queue by reference
                logger.debug(f"[{url}] Extracting links")
                if depth < max_depth:
                    extract_links(parent_link, r.text, depth, input_queue)

            res = RawData(link=url, parent_link=parent_link, content=r.text, scraped_at=datetime.now())
            output_queue.put(res)
            logger.debug(f"[{url}] Complete extraction")
            stats.inc_extracted()
        except requests.exceptions.RequestException as e:
            logger.error(f"[{url}] RequestException: {e}")
            stats.dlq_append(("extract", url, e))
        except Exception as e:
            logger.error(f"[{url}] Exception: {e}")
            stats.dlq_append(("extract", url, e))
        finally:
            # Runs even if a page errors out
            input_queue.task_done()
