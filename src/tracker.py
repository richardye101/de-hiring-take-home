import threading
import time
import logging

logger = logging.getLogger()


class CrawlStats:
    def __init__(self):
        self.start_time = 0
        self.end_time_extract = 0
        self.end_time_total = 0
        self._extracted = 0
        self._transformed = 0
        self._loaded = 0
        # Store pages that errored as a tuple: (stage, url, error)
        self._dlq = set()
        self._lock = threading.Lock()

    def start(self):
        self.start_time = time.time()

    def end_extract(self):
        self.end_time_extract = time.time()

    def end(self):
        self.end_time_total = time.time()

    def inc_extracted(self):
        with self._lock:
            self._extracted += 1

    def inc_transformed(self):
        with self._lock:
            self._transformed += 1

    def inc_loaded(self, count=1):
        with self._lock:
            self._loaded += count

    def dlq_append(self, item: tuple[str, str]):
        with self._lock:
            self._dlq.add(item)

    def report(self):
        with self._lock:
            return {
                "extracted": self._extracted,
                "transformed": self._transformed,
                "loaded": self._loaded,
                "dlq_size": len(self._dlq),
                "elapsed_time": time.time() - self.start_time,
            }

    def get_dlq(self):
        with self._lock:
            return self._dlq


def monitor_worker(stats, stop_event):
    while not stop_event.is_set():
        data = stats.report()
        extract_per_min = round(data["extracted"] / data["elapsed_time"] * 60, 2)
        transform_per_min = round(data["transformed"] / data["elapsed_time"] * 60, 2)
        load_per_min = round(data["loaded"] / data["elapsed_time"] * 60, 2)
        # \r keeps the output on one line in the terminal
        msg = f"\rSeconds Elapsed: {data['elapsed_time']} [Extract] {data['extracted']} links, {extract_per_min}/min | [Tranform] {data['transformed']}, {transform_per_min}/min | [Load] {data['loaded']}, {load_per_min}/min"
        msg = (
            f"\r[{round(data['elapsed_time'],2):>10}s] "
            f"Extract: {data['extracted']:>8} ({extract_per_min:>8}/m) | "
            f"Transform: {data['transformed']:>8} ({transform_per_min:>9}/m) | "
            f"Load: {data['loaded']:>8} ({load_per_min:>9}/m)"
        )
        # To track the status throughout the logs as well
        logger.info(msg)
        # print(msg, end="")
        time.sleep(1)
