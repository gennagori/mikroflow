import logging
import queue
import socket
import threading
import time

from mikroflow.collector.parser import TemplateStore, parse_v9

log = logging.getLogger("mikroflow.collector.receiver")

REPORT_INTERVAL_SECONDS = 10


class UdpReceiver(threading.Thread):
    def __init__(self, host, port, recv_buffer_bytes, out_queue, store=None):
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._recv_buffer_bytes = recv_buffer_bytes
        self._queue = out_queue
        self._store = store or TemplateStore()
        self._stop_event = threading.Event()

    def _make_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_bytes)
        except OSError:
            pass
        s.bind((self._host, self._port))
        s.settimeout(1.0)
        return s

    def run(self) -> None:
        sock = self._make_socket()
        log.info("NetFlow receiver listening on %s:%s", self._host, self._port)
        datagrams = 0
        flows_total = 0
        dropped = 0
        versions: set[int] = set()
        last_report = time.monotonic()
        try:
            while not self._stop_event.is_set():
                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                datagrams += 1
                if len(data) >= 2:
                    versions.add(int.from_bytes(data[:2], "big"))
                flows = parse_v9(data, addr[0], self._store)
                flows_total += len(flows)
                for flow in flows:
                    try:
                        self._queue.put_nowait(flow)
                    except queue.Full:
                        dropped += 1  # bounded queue: drop under sustained overload
                now = time.monotonic()
                if now - last_report >= REPORT_INTERVAL_SECONDS:
                    log.info(
                        "datagrams=%d parsed_flows=%d dropped=%d versions_seen=%s queue=%d",
                        datagrams, flows_total, dropped, sorted(versions),
                        self._queue.qsize(),
                    )
                    last_report = now
        finally:
            sock.close()

    def stop(self) -> None:
        self._stop_event.set()
