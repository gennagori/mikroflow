import queue
import threading
import time

from mikroflow.sinks import FlowSink


class BatchWriter(threading.Thread):
    def __init__(self, in_queue, sink: FlowSink, batch_size: int, flush_seconds: float):
        super().__init__(daemon=True)
        self._queue = in_queue
        self._sink = sink
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._stop_event = threading.Event()

    def run(self) -> None:
        buf = []
        last = time.monotonic()
        while not self._stop_event.is_set() or not self._queue.empty():
            timeout = max(0.0, self._flush_seconds - (time.monotonic() - last))
            try:
                buf.append(self._queue.get(timeout=timeout))
            except queue.Empty:
                pass
            due = len(buf) >= self._batch_size or (time.monotonic() - last) >= self._flush_seconds
            if buf and due:
                self._sink.write_batch(buf)
                buf = []
                last = time.monotonic()
        if buf:
            self._sink.write_batch(buf)

    def stop(self) -> None:
        self._stop_event.set()
