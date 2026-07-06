-- Raw flows: partitioned by day, short retention.
CREATE TABLE IF NOT EXISTS flows_raw (
    ts          timestamptz NOT NULL,
    src_ip      inet        NOT NULL,
    dst_ip      inet        NOT NULL,
    src_port    integer     NOT NULL,
    dst_port    integer     NOT NULL,
    protocol    smallint    NOT NULL,
    bytes       bigint      NOT NULL,
    packets     bigint      NOT NULL,
    exporter_ip inet        NOT NULL
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS flows_raw_ts_idx ON flows_raw (ts);
CREATE INDEX IF NOT EXISTS flows_raw_dst_idx ON flows_raw (dst_ip);

-- DHCP leases with history (valid_to IS NULL == currently active).
CREATE TABLE IF NOT EXISTS dhcp_leases (
    ip         inet        NOT NULL,
    mac        text,
    hostname   text,
    valid_from timestamptz NOT NULL,
    valid_to   timestamptz
);
CREATE INDEX IF NOT EXISTS dhcp_leases_ip_idx ON dhcp_leases (ip, valid_from);
CREATE UNIQUE INDEX IF NOT EXISTS dhcp_leases_open_uidx
    ON dhcp_leases (ip) WHERE valid_to IS NULL;

-- ARP snapshot: broad IP -> MAC map covering static hosts, not just DHCP.
CREATE TABLE IF NOT EXISTS arp (
    ip         inet PRIMARY KEY,
    mac        text,
    updated_at timestamptz NOT NULL
);

-- Manual MAC -> human-friendly name overrides (static hosts, servers). Filled
-- by hand; takes priority over the DHCP hostname. Matched case-insensitively.
CREATE TABLE IF NOT EXISTS device_alias (
    mac  text PRIMARY KEY,
    name text NOT NULL
);

-- Reverse-DNS cache.
CREATE TABLE IF NOT EXISTS ip_domain (
    ip          inet PRIMARY KEY,
    domain      text,
    status      text        NOT NULL,   -- 'ok' | 'nxdomain'
    resolved_at timestamptz NOT NULL,
    ttl         integer     NOT NULL
);

-- True for RFC1918 private addresses (the LAN side of a connection).
CREATE OR REPLACE FUNCTION is_private(addr inet) RETURNS boolean AS $$
    SELECT addr << inet '10.0.0.0/8'
        OR addr << inet '172.16.0.0/12'
        OR addr << inet '192.168.0.0/16';
$$ LANGUAGE sql IMMUTABLE;

-- Auto-migrate away from the old hourly-aggregate layout: it grouped flows by
-- hour and folded both directions into one row, which threw away each flow's
-- exact timestamp. Drop it unconditionally so the per-flow table below takes
-- its place. No-op once a DB has already been migrated (table won't exist).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_name = 'flows_hourly') THEN
        DROP TABLE flows_hourly CASCADE;
        DELETE FROM agg_state WHERE name = 'hourly';
    END IF;
END $$;

-- Processed flows: partitioned by month, long retention. Main analysis table.
-- This is a straight, un-aggregated copy of flows_raw (same columns, same
-- one-row-per-flow granularity, exact original ts preserved) with three
-- enrichment columns added: device_name/mac for whichever side of the flow
-- is the LAN endpoint, and remote_domain (reverse-DNS) for the other side.
CREATE TABLE IF NOT EXISTS flows_processed (
    ts            timestamptz NOT NULL,
    src_ip        inet        NOT NULL,
    dst_ip        inet        NOT NULL,
    src_port      integer     NOT NULL,
    dst_port      integer     NOT NULL,
    protocol      smallint    NOT NULL,
    bytes         bigint      NOT NULL,
    packets       bigint      NOT NULL,
    exporter_ip   inet        NOT NULL,
    device_name   text,
    mac           text,
    remote_domain text
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS flows_processed_ts_idx ON flows_processed (ts);

-- Processing watermark (how far into flows_raw we've copied/enriched).
CREATE TABLE IF NOT EXISTS agg_state (
    name      text PRIMARY KEY,
    last_hour timestamptz
);

DROP VIEW IF EXISTS v_connections;
CREATE VIEW v_connections AS
SELECT
    ts,
    device_name,
    mac,
    CASE WHEN is_private(dst_ip) AND NOT is_private(src_ip)
         THEN dst_ip ELSE src_ip END AS device_ip,
    remote_domain,
    CASE WHEN is_private(dst_ip) AND NOT is_private(src_ip)
         THEN src_ip ELSE dst_ip END AS remote_ip,
    CASE WHEN is_private(dst_ip) AND NOT is_private(src_ip)
         THEN src_port ELSE dst_port END AS remote_port,
    protocol,
    bytes,
    packets,
    exporter_ip
FROM flows_processed;

CREATE OR REPLACE FUNCTION ensure_daily_partition(day date) RETURNS void AS $$
DECLARE part text := 'flows_raw_' || to_char(day, 'YYYYMMDD');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF flows_raw FOR VALUES FROM (%L) TO (%L)',
            part, day::timestamptz, (day + 1)::timestamptz);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_monthly_partition(mon date) RETURNS void AS $$
DECLARE
    first date := date_trunc('month', mon)::date;
    part  text := 'flows_processed_' || to_char(first, 'YYYYMM');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF flows_processed FOR VALUES FROM (%L) TO (%L)',
            part, first::timestamptz, (first + interval '1 month')::timestamptz);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_partition_window(days_ahead int, months_ahead int)
RETURNS void AS $$
DECLARE d int; m int;
BEGIN
    FOR d IN -1 .. days_ahead LOOP
        PERFORM ensure_daily_partition((current_date + d));
    END LOOP;
    FOR m IN -1 .. months_ahead LOOP
        PERFORM ensure_monthly_partition(
            (date_trunc('month', current_date) + (m || ' month')::interval)::date);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Postgres refuses to CREATE OR REPLACE a function if a parameter name
-- changes (hourly_days -> processed_days here), so drop the old signature
-- explicitly first. Safe/no-op on fresh databases (IF EXISTS).
DROP FUNCTION IF EXISTS drop_old_partitions(int, int);

CREATE OR REPLACE FUNCTION drop_old_partitions(raw_days int, processed_days int)
RETURNS void AS $$
DECLARE
    r record;
    cutoff_raw       date := current_date - raw_days;
    cutoff_processed date := date_trunc('month', current_date - processed_days)::date;
BEGIN
    FOR r IN SELECT relname FROM pg_class
             WHERE relkind = 'r' AND relname ~ '^flows_raw_[0-9]{8}$' LOOP
        IF to_date(right(r.relname, 8), 'YYYYMMDD') < cutoff_raw THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
    FOR r IN SELECT relname FROM pg_class
             WHERE relkind = 'r' AND relname ~ '^flows_processed_[0-9]{6}$' LOOP
        IF to_date(right(r.relname, 6) || '01', 'YYYYMMDD') < cutoff_processed THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
