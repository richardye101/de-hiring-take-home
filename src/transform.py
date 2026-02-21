import re
import logging

from bs4 import BeautifulSoup
from datetime import datetime
from queue import Queue

from src.tracker import CrawlStats
from src.models import RawData, TransformedData

logger = logging.getLogger(__name__)


def parse_wiki_date(text: str) -> datetime:
    """
    Parses: 'This page was last edited on 19 February 2026, at 01:01 (UTC).'
    """
    try:
        # Extract the date and time part
        # Match: 19 February 2026, at 01:01
        match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}),\s+at\s+(\d{2}:\d{2})", text)
        if match:
            date_part = match.group(1)  # 19 February 2026
            time_part = match.group(2)  # 01:01
            full_str = f"{date_part} {time_part}"
            return datetime.strptime(full_str, "%d %B %Y %H:%M")
    except Exception as e:
        logger.debug(f"Date parsing failed for string '{text}': {e}")

    return datetime.now()


def transform_worker(input_queue: Queue[RawData], output_queue: Queue[TransformedData], stats: CrawlStats):
    logger.info("Worker started")
    while True:
        # Outer try/except increases readability, and i've designed it such that the data being extracted have default values to fall back to
        url = ""
        try:
            raw_data = input_queue.get()
            if raw_data is None:
                logger.info("Worker received shutdown signal")
                break

            body = raw_data.content
            url = raw_data.link
            parent_link = raw_data.parent_link
            scraped_at = raw_data.scraped_at
            logger.debug(f"Start transform on {url}")

            try:
                soup = BeautifulSoup(body, "html.parser")
            except TypeError as e:
                logger.warning(f"[{url}] Body Content could not be parsed: {e}")
                continue

            # 1. Title Extraction
            title_tag = soup.find(id="mw-page-title-main") or soup.find(id="firstHeading")
            title_text = title_tag.get_text(strip=True) if title_tag else "Unknown Title"
            logger.debug(f"[{url}] Title: {title_text}")

            # 2. Last Modified Date, defaults to now
            footer_mod = soup.find(id="footer-info-lastmod")
            last_mod = parse_wiki_date(footer_mod.get_text()) if footer_mod else datetime.now()
            logger.debug(f"[{url}] Last Modified: {last_mod}")

            # 3. References (Using your specific class)
            # We find the wrap, then count all list items inside it
            ref_wrap = soup.find("div", class_=lambda x: x and "mw-references-wrap" in x)
            num_refs = len(ref_wrap.find_all("li")) if ref_wrap else 0
            logger.debug(f"[{url}] Number of References: {num_refs}")

            # 4. Text Content & Word Count
            # Remove non-prose elements to get a 'clean' word count
            body_content = soup.find(class_="mw-content-container")
            if body_content:
                num_links = len(body_content.find_all("a", href=True))
                num_h2 = len(body_content.find_all("h2"))

                # Remove visual/meta noise for clean word count
                for tag in body_content(["style", "script", "table", "figure", "div", "sup"]):
                    tag.decompose()

                full_text = body_content.get_text(separator=" ", strip=True)
                word_count = len(full_text.split())
            else:
                full_text = ""
                num_links = 0
                num_h2 = 0
                word_count = 0

            logger.debug(f"[{url}] Number of Links on Page: {num_links}")
            logger.debug(f"[{url}] Number of H2 Headings: {num_h2}")
            logger.debug(f"[{url}] Word Count: {word_count}")

            # 6. Pydantic Model Creation
            result = TransformedData(
                link=url,
                parent_link=parent_link,
                title=title_text,
                content=full_text,
                num_links=num_links,
                num_h2=num_h2,
                num_refs=num_refs,
                word_count=word_count,
                modified_at_utc=last_mod,
                scraped_at=scraped_at,
            )
            logger.debug(f"[{url}] Completed Transform")
            output_queue.put(result)
            stats.inc_transformed()
        except Exception as e:
            logger.warning(e)
            stats.dlq_append(("transform", url, e))
        finally:
            input_queue.task_done()
