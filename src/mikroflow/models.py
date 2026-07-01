from dataclasses import dataclass
from datetime import datetime


@dataclass
class Flow:
    ts: datetime
    src_ip: str | None
    dst_ip: str | None
    src_port: int
    dst_port: int
    protocol: int
    bytes: int
    packets: int
    exporter_ip: str
