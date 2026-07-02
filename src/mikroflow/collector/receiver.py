import queue
import socket
import threading

from mikroflow.collector.parser import TemplateStore, parse_v9


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
        try:
            while not self._stop_event.is_set():
                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                for flow in parse_v9(data, addr[0], self._store):
                    try:
                        self._queue.put_nowait(flow)
                    except queue.Full:
                        pass  # bounded queue: drop under sustained overload
        finally:
            sock.close()

    def stop(self) -> None:
        self._stop_event.set()
