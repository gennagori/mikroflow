from unittest.mock import MagicMock

from mikroflow.config import Settings
from mikroflow.worker.main import build_scheduler


def test_scheduler_registers_all_jobs():
    pool = MagicMock()
    sched = build_scheduler(pool, Settings())
    names = {job.name for job in sched.get_jobs()}
    assert names == {"lease_sync", "rdns", "aggregate", "maintenance"}
