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

-- Reverse-DNS cache.
CREATE TABLE IF NOT EXISTS ip_domain (
    ip          inet PRIMARY KEY,
    domain      text,
    status      text        NOT NULL,   -- 'ok' | 'nxdomain'
    resolved_at timestamptz NOT NULL,
    ttl         integer     NOT NULL
);

-- Hourly aggregates: partitioned by month, 6-month retention. Main analysis table.
CREATE TABLE IF NOT EXISTS flows_hourly (
    hour        timestamptz NOT NULL,
    src_ip      inet        NOT NULL,
    device_name text,
    dst_ip      inet        NOT NULL,
    dst_domain  text,
    dst_port    integer     NOT NULL,
    protocol    smallint    NOT NULL,
    bytes       bigint      NOT NULL,
    packets     bigint      NOT NULL,
    flow_count  bigint      NOT NULL,
    PRIMARY KEY (hour, src_ip, dst_ip, dst_port, protocol)
) PARTITION BY RANGE (hour);

-- Aggregation watermark.
CREATE TABLE IF NOT EXISTS agg_state (
    name      text PRIMARY KEY,
    last_hour timestamptz
);

CREATE OR REPLACE VIEW v_connections AS
SELECT hour, src_ip, device_name, dst_ip, dst_domain, dst_port, protocol,
       bytes, packets, flow_count
FROM flows_hourly;

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
    part  text := 'flows_hourly_' || to_char(first, 'YYYYMM');
BEGIN
    IF to_regclass(part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF flows_hourly FOR VALUES FROM (%L) TO (%L)',
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

CREATE OR REPLACE FUNCTION drop_old_partitions(raw_days int, hourly_days int)
RETURNS void AS $$
DECLARE
    r record;
    cutoff_raw    date := current_date - raw_days;
    cutoff_hourly date := date_trunc('month', current_date - hourly_days)::date;
BEGIN
    FOR r IN SELECT relname FROM pg_class
             WHERE relkind = 'r' AND relname ~ '^flows_raw_[0-9]{8}$' LOOP
        IF to_date(right(r.relname, 8), 'YYYYMMDD') < cutoff_raw THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
    FOR r IN SELECT relname FROM pg_class
             WHERE relkind = 'r' AND relname ~ '^flows_hourly_[0-9]{6}$' LOOP
        IF to_date(right(r.relname, 6) || '01', 'YYYYMMDD') < cutoff_hourly THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', r.relname);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
