import queue
from datetime import datetime, timezone

from mikroflow.collector.writer import BatchWriter
from mikroflow.models import Flow
from mikroflow.sinks import FlowSink


class MemorySink(FlowSink):
    def __init__(self):
        self.batches = []

    def write_batch(self, flows):
        if flows:
            self.batches.append(list(flows))


def _flow():
    return Flow(datetime.now(timezone.utc), "10.0.0.2", "8.8.8.8", 1, 2, 6, 10, 1, "10.0.0.1")


def test_writer_flushes_by_batch_size():
    q = queue.Queue()
    sink = MemorySink()
    for _ in range(5):
        q.put(_flow())
    w = BatchWriter(q, sink, batch_size=5, flush_seconds=30.0)
    w.start()
    w.stop()
    w.join(timeout=5)
    assert sum(len(b) for b in sink.batches) == 5
