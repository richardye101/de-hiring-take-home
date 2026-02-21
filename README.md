# Read Me

My implementation of a wikipedia crawler utilizes a producer-consumer architecture with thread-safe queues to process data across three distinct stages.

## Setup Instructions

1. Install uv:

```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Sync with the project to install all the dependencies:

```
uv sync
```

3. Copy `.env.example` as `.env`, you shouldn't have to change any variables:

```
cp .env.example .env
```

## Run the Pipeline

The pipeline is executed via main.py with several configurable arguments:
Arguments:

    -w, --webpage: The starting Wikipedia URL.

    -d, --depth: How many levels of links to follow (default: 2).

    -e, --extract_workers: Concurrent threads for network requests.

    -t, --transform_workers: Concurrent threads for HTML parsing.

    -v, --verbose: Enables DEBUG logging for detailed internal tracking.

I found the sweet spot config is 10 extract workers, and 5 transform workers:

```
uv run main.py --webpage "https://en.wikipedia.org/wiki/Toronto" --depth 2 --extract_workers 10 --transform_workers 5
```

or

```
uv run main.py --webpage "https://en.wikipedia.org/wiki/Toronto" -d 1 -e 10 -t 5
```

## Data Schema

The project uses SQLModel (built on SQLAlchemy and Pydantic) for data integrity. It's also used to pass data between the transformer and loader worker.

`TransformedData` Table
| Field | Type | Description |
| :--- | :--- | :--- |
| **link** (PK) | `String` | Unique Wikipedia URL. |
| **parent_link** | `String` | The URL where this link was discovered. |
| **title** | `String` | Page title (Indexed). |
| **content** | `String` | Cleaned text (noise/tables/scripts removed). |
| **num_links** | `Integer` | Count of outgoing links in the main content. |
| **num_h2** | `Integer` | Count of H2 headings. |
| **num_refs** | `Integer` | Count of citations in the references section. |
| **word_count** | `Integer` | Number of words in cleaned content. |
| **scraped_at** | `DateTime` | Timestamp of the crawl. |
| **modified_at_utc** | `DateTime` | Last edited date extracted from the Wiki footer. |

## Assumptions and Design Decisions

1. Multi-Stage Pipeline (Fan-In/Fan-Out)

The project uses three specialized worker types connected by `queue.Queue` objects:

- Extractors: I/O bound. Perform network requests and URL discovery. Traverses webpages using BFS until `max_depth` is reached.
- Transformers: CPU bound. Use BeautifulSoup to clean and parse HTML. It uses `html.parser` because does not have external dependencies like `lxml`.
- Loader: Disk I/O bound. A single worker batches records to SQLite to avoid "Database Locked" errors.

2. Resilience and Error Handling
   - Poison Pills: Shutdown signals (`None`) are passed through queues to ensure workers finish their current task and close gracefully after the main thread calls .join() for each `Queue` being watched by each type of worker.
   - Dead Letter Queue (DLQ): Errors (Timeouts, IntegrityErrors) are captured in a thread-safe set within CrawlStats rather than crashing the thread.
   - Retries: The extraction layer uses `urllib3` retry logic with exponential backoff to handle transient 502/503 errors.

3. Performance Optimizations

- Visited Set: A global thread-safe set prevents the extractor workers from requesting the same URL multiple times.
- Batch Loading: The load_worker collects records and uses session.merge() for "Upsert" logic, committing in chunks to minimize disk overhead.
- Rate Limiting: A global limiter ensures compliance with Wikipedia's bot policies by spacing out requests.

4. Data Cleaning Logic

The transformer specifically targets `div.mw-content-container` and decomposes <table>, <script>, and <style> tags before calculating word counts to ensure "prose-only" data metrics.

5. Database choice

I chose SQLite because it provides the ACID compliance and relational indexing of a professional database without the overhead of managing a separate database server, making the pipeline portable and robust against crashes.
