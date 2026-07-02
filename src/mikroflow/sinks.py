from abc import ABC, abstractmethod
from collections.abc import Sequence

from psycopg_pool import ConnectionPool

from mikroflow.models import Flow

_COPY_SQL = (
    "COPY flows_raw (ts, src_ip, dst_ip, src_port, dst_port, protocol, "
    "bytes, packets, exporter_ip) FROM STDIN"
)


class FlowSink(ABC):
    @abstractmethod
    def write_batch(self, flows: Sequence[Flow]) -> None: ...


class PostgresFlowSink(FlowSink):
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def write_batch(self, flows: Sequence[Flow]) -> None:
        if not flows:
            return
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                with cur.copy(_COPY_SQL) as copy:
                    for f in flows:
                        copy.write_row(
                            (
                                f.ts, f.src_ip, f.dst_ip, f.src_port, f.dst_port,
                                f.protocol, f.bytes, f.packets, f.exporter_ip,
                            )
                        )
