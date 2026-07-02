import queue
import signal

from mikroflow.collector.receiver import UdpReceiver
from mikroflow.collector.writer import BatchWriter
from mikroflow.config import get_settings
from mikroflow.db import apply_schema, ensure_partitions, make_pool
from mikroflow.sinks import PostgresFlowSink


def main() -> None:
    s = get_settings()
    pool = make_pool(s.db_dsn)
    apply_schema(pool)
    ensure_partitions(pool, s.partition_days_ahead, s.partition_months_ahead)

    q: queue.Queue = queue.Queue(maxsize=s.queue_maxsize)
    sink = PostgresFlowSink(pool)
    writer = BatchWriter(q, sink, s.batch_size, s.batch_flush_seconds)
    receiver = UdpReceiver(s.netflow_host, s.netflow_port, s.recv_buffer_bytes, q)

    writer.start()
    receiver.start()
    signal.sigwait([signal.SIGINT, signal.SIGTERM])

    receiver.stop()
    writer.stop()
    receiver.join(timeout=10)
    writer.join(timeout=10)
    pool.close()


if __name__ == "__main__":
    main()
