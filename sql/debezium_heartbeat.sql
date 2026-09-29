-- Run once in order_db BEFORE starting cdc-app.
-- Debezium writes here every heartbeat.interval.ms (heartbeat.action.query). The table is part of
-- the publication, so each write reaches the connector, which then confirms the latest LSN and lets
-- Postgres recycle WAL even when order_entity has no changes.
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id INT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL
);
