from apscheduler.schedulers.blocking import BlockingScheduler

from mikroflow.config import get_settings
from mikroflow.db import apply_schema, make_pool
from mikroflow.worker.aggregator import aggregate
from mikroflow.worker.lease_sync import fetch_leases, normalize_leases, sync_leases
from mikroflow.worker.rdns import resolve_pending
from mikroflow.worker.retention import run_maintenance


def _do_lease_sync(pool, s):
    rows = fetch_leases(s.router_host, s.router_user, s.router_password, s.router_port)
    sync_leases(pool, normalize_leases(rows))


def build_scheduler(pool, s):
    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(lambda: _do_lease_sync(pool, s), "interval",
                  seconds=s.lease_sync_seconds, id="lease_sync", name="lease_sync")
    sched.add_job(lambda: resolve_pending(pool, s), "interval",
                  seconds=s.rdns_poll_seconds, id="rdns", name="rdns")
    sched.add_job(lambda: aggregate(pool), "interval",
                  seconds=s.aggregate_seconds, id="aggregate", name="aggregate")
    sched.add_job(lambda: run_maintenance(pool, s), "interval",
                  hours=1, id="maintenance", name="maintenance")
    return sched


def main():
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    s = get_settings()
    pool = make_pool(s.db_dsn)
    apply_schema(pool)
    run_maintenance(pool, s)
    build_scheduler(pool, s).start()


if __name__ == "__main__":
    main()
