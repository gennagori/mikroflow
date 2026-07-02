import queue
import socket
import struct
import time

from mikroflow.collector.receiver import UdpReceiver
from mikroflow.collector.writer import BatchWriter
from mikroflow.sinks import PostgresFlowSink

FIELDS = [(8, 4), (12, 4), (7, 2), (11, 2), (4, 1), (1, 4), (2, 4)]


def _template(tid=256, source_id=1):
    body = struct.pack("!HH", tid, len(FIELDS))
    for ft, fl in FIELDS:
        body += struct.pack("!HH", ft, fl)
    fs = struct.pack("!HH", 0, 4 + len(body)) + body
    return struct.pack("!HHIIII", 9, 1, 0, 0, 1, source_id) + fs


def _data(tid=256, source_id=1):
    rec = (bytes([192, 168, 1, 10]) + bytes([8, 8, 8, 8])
           + struct.pack("!H", 5000) + struct.pack("!H", 443)
           + bytes([6]) + struct.pack("!I", 1500) + struct.pack("!I", 12))
    fs = struct.pack("!HH", tid, 4 + len(rec)) + rec
    return struct.pack("!HHIIII", 9, 1, 0, 0, 2, source_id) + fs


def test_udp_to_postgres(pool):
    q = queue.Queue(maxsize=1000)
    writer = BatchWriter(q, PostgresFlowSink(pool), batch_size=1, flush_seconds=0.2)
    # bind to an ephemeral port by pre-creating the socket
    probe = UdpReceiver("127.0.0.1", 0, 1 << 20, q)
    sock = probe._make_socket()
    port = sock.getsockname()[1]
    sock.close()
    receiver = UdpReceiver("127.0.0.1", port, 1 << 20, q)
    writer.start()
    receiver.start()
    try:
        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx.sendto(_template(), ("127.0.0.1", port))
        time.sleep(0.2)
        tx.sendto(_data(), ("127.0.0.1", port))
        deadline = time.time() + 5
        rows = 0
        while time.time() < deadline:
            with pool.connection() as conn:
                rows = conn.execute("SELECT count(*) FROM flows_raw").fetchone()[0]
            if rows:
                break
            time.sleep(0.1)
        assert rows == 1
    finally:
        receiver.stop()
        writer.stop()
        receiver.join(timeout=5)
        writer.join(timeout=5)
