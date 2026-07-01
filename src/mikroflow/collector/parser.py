import struct
from datetime import datetime, timezone
from ipaddress import IPv4Address

from mikroflow.models import Flow

FIELD_IN_BYTES = 1
FIELD_IN_PKTS = 2
FIELD_PROTOCOL = 4
FIELD_L4_SRC_PORT = 7
FIELD_IPV4_SRC_ADDR = 8
FIELD_L4_DST_PORT = 11
FIELD_IPV4_DST_ADDR = 12


class TemplateStore:
    def __init__(self) -> None:
        self._templates: dict[tuple[str, int, int], list[tuple[int, int]]] = {}

    def set(self, exporter_ip, source_id, template_id, fields):
        self._templates[(exporter_ip, source_id, template_id)] = fields

    def get(self, exporter_ip, source_id, template_id):
        return self._templates.get((exporter_ip, source_id, template_id))


def _int(data: bytes) -> int:
    return int.from_bytes(data, "big") if data else 0


def parse_v9(datagram, exporter_ip, store, now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    flows: list[Flow] = []
    n = len(datagram)
    if n < 20:
        return flows
    version, count, uptime, secs, seq, source_id = struct.unpack("!HHIIII", datagram[:20])
    if version != 9:
        return flows
    offset = 20
    while offset + 4 <= n:
        flowset_id, length = struct.unpack("!HH", datagram[offset:offset + 4])
        if length < 4 or offset + length > n:
            break
        body = datagram[offset + 4:offset + length]
        if flowset_id == 0:
            _parse_templates(body, exporter_ip, source_id, store)
        elif flowset_id > 255:
            flows.extend(_parse_data(body, exporter_ip, source_id, flowset_id, store, now))
        offset += length
    return flows


def _parse_templates(body, exporter_ip, source_id, store):
    o = 0
    while o + 4 <= len(body):
        template_id, field_count = struct.unpack("!HH", body[o:o + 4])
        o += 4
        fields = []
        for _ in range(field_count):
            if o + 4 > len(body):
                return
            ftype, flen = struct.unpack("!HH", body[o:o + 4])
            fields.append((ftype, flen))
            o += 4
        store.set(exporter_ip, source_id, template_id, fields)


def _parse_data(body, exporter_ip, source_id, template_id, store, now):
    fields = store.get(exporter_ip, source_id, template_id)
    if not fields:
        return []
    rec_len = sum(flen for _, flen in fields)
    if rec_len == 0:
        return []
    flows = []
    o = 0
    while o + rec_len <= len(body):
        values: dict[int, bytes] = {}
        p = o
        for ftype, flen in fields:
            values[ftype] = body[p:p + flen]
            p += flen
        o += rec_len
        flows.append(_to_flow(values, exporter_ip, now))
    return flows


def _to_flow(values, exporter_ip, now):
    def ip(t):
        b = values.get(t)
        return str(IPv4Address(b)) if b and len(b) == 4 else None

    return Flow(
        ts=now,
        src_ip=ip(FIELD_IPV4_SRC_ADDR),
        dst_ip=ip(FIELD_IPV4_DST_ADDR),
        src_port=_int(values.get(FIELD_L4_SRC_PORT, b"")),
        dst_port=_int(values.get(FIELD_L4_DST_PORT, b"")),
        protocol=_int(values.get(FIELD_PROTOCOL, b"")),
        bytes=_int(values.get(FIELD_IN_BYTES, b"")),
        packets=_int(values.get(FIELD_IN_PKTS, b"")),
        exporter_ip=exporter_ip,
    )
