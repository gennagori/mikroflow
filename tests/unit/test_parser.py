import struct

from mikroflow.collector.parser import TemplateStore, parse_v9

# NetFlow v9 field types: SRC_IP=8, DST_IP=12, L4_SRC_PORT=7, L4_DST_PORT=11,
# PROTOCOL=4, IN_BYTES=1, IN_PKTS=2
FIELDS = [(8, 4), (12, 4), (7, 2), (11, 2), (4, 1), (1, 4), (2, 4)]


def build_v9_template(template_id, fields, source_id=1, seq=1):
    body = struct.pack("!HH", template_id, len(fields))
    for ftype, flen in fields:
        body += struct.pack("!HH", ftype, flen)
    flowset = struct.pack("!HH", 0, 4 + len(body)) + body
    header = struct.pack("!HHIIII", 9, 1, 0, 0, seq, source_id)
    return header + flowset


def build_v9_data(template_id, records, source_id=1, seq=2):
    body = b"".join(records)
    flowset = struct.pack("!HH", template_id, 4 + len(body)) + body
    header = struct.pack("!HHIIII", 9, len(records), 0, 0, seq, source_id)
    return header + flowset


def make_record():
    return (
        bytes([192, 168, 1, 10]) + bytes([8, 8, 8, 8])
        + struct.pack("!H", 54321) + struct.pack("!H", 443)
        + bytes([6]) + struct.pack("!I", 1500) + struct.pack("!I", 12)
    )


def test_parse_data_after_template():
    store = TemplateStore()
    parse_v9(build_v9_template(256, FIELDS), "10.0.0.1", store)
    flows = parse_v9(build_v9_data(256, [make_record(), make_record()]), "10.0.0.1", store)
    assert len(flows) == 2
    f = flows[0]
    assert f.src_ip == "192.168.1.10"
    assert f.dst_ip == "8.8.8.8"
    assert f.src_port == 54321
    assert f.dst_port == 443
    assert f.protocol == 6
    assert f.bytes == 1500
    assert f.packets == 12
    assert f.exporter_ip == "10.0.0.1"


def test_data_without_template_is_skipped():
    store = TemplateStore()
    flows = parse_v9(build_v9_data(256, [make_record()]), "10.0.0.1", store)
    assert flows == []
