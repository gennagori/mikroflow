import logging
import queue
import threading
import time

from mikroflow.sinks import FlowSink

log = logging.getLogger("mikroflow.collector.writer")

REPORT_INTERVAL_SECONDS = 10


class BatchWriter(threading.Thread):
    def __init__(self, in_queue, sink: FlowSink, batch_size: int, flush_seconds: float):
        super().__init__(daemon=True)
        self._queue = in_queue
        self._sink = sink
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._stop_event = threading.Event()

    def _flush(self, buf, written):
        try:
            self._sink.write_batch(buf)
            return written + len(buf)
        except Exception:
            log.exception("failed to write batch of %d flows", len(buf))
            return written

    def run(self) -> None:
        buf = []
        written = 0
        last = time.monotonic()
        last_report = time.monotonic()
        while not self._stop_event.is_set() or not self._queue.empty():
            timeout = max(0.0, self._flush_seconds - (time.monotonic() - last))
            try:
                buf.append(self._queue.get(timeout=timeout))
            except queue.Empty:
                pass
            due = len(buf) >= self._batch_size or (time.monotonic() - last) >= self._flush_seconds
            if buf and due:
                written = self._flush(buf, written)
                buf = []
                last = time.monotonic()
            now = time.monotonic()
            if now - last_report >= REPORT_INTERVAL_SECONDS:
                log.info("written_rows=%d queue=%d", written, self._queue.qsize())
                last_report = now
        if buf:
            self._flush(buf, written)

    def stop(self) -> None:
        self._stop_event.set()
