from mikroflow.worker.lease_sync import normalize_leases


def test_normalize_prefers_active_fields_and_hostname():
    rows = [
        {"active-address": "192.168.1.10", "active-mac-address": "AA:BB",
         "host-name": "laptop", "address": "192.168.1.10"},
        {"address": "192.168.1.11", "mac-address": "CC:DD", "comment": "printer"},
        {"mac-address": "EE:FF"},  # no ip -> skipped
    ]
    assert normalize_leases(rows) == [
        ("192.168.1.10", "AA:BB", "laptop"),
        ("192.168.1.11", "CC:DD", "printer"),
    ]
