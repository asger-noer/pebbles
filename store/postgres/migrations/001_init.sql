CREATE SCHEMA IF NOT EXISTS pebbles;

CREATE TABLE pebbles.event (
    id              UUID        PRIMARY KEY,
    stream_type     TEXT        NOT NULL,
    stream_id       UUID        NOT NULL,
    sequence        BIGINT      NOT NULL,
    kind            TEXT        NOT NULL,
    version         INTEGER     NOT NULL,
    content_type    TEXT        NOT NULL,
    at              TIMESTAMPTZ NOT NULL,
    headers         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    data            BYTEA       NOT NULL,
    -- global_position is the total order projections read by. xact_id is the
    -- commit-safety horizon: a cross-stream read only returns rows whose writing
    -- transaction is guaranteed complete, so a low position that commits late is
    -- never skipped.
    global_position BIGINT      GENERATED ALWAYS AS IDENTITY,
    xact_id         xid8        NOT NULL DEFAULT pg_current_xact_id(),
    UNIQUE (stream_type, stream_id, sequence)
);

CREATE INDEX event_stream_type_idx ON pebbles.event (stream_type);
CREATE INDEX event_stream_idx ON pebbles.event (stream_type, stream_id, sequence);
CREATE INDEX event_global_position_idx ON pebbles.event (global_position);

-- Cursor for raw Handler-based projections (position only).
CREATE TABLE pebbles.projection_checkpoint (
    name     TEXT   PRIMARY KEY,
    position BIGINT NOT NULL DEFAULT 0
);

-- Keyed projection: one model per grouping key. The cursor lives in
-- projection_checkpoint; models, index, and cursor are committed in one tx.
CREATE TABLE pebbles.projection_keyed_snapshot (
    name  TEXT  NOT NULL,
    key   UUID  NOT NULL,
    model JSONB NOT NULL,
    PRIMARY KEY (name, key)
);

-- Keyed projection resolver index: learned id -> grouping key mappings.
CREATE TABLE pebbles.projection_keyed_index (
    name TEXT NOT NULL,
    id   UUID NOT NULL,
    key  UUID NOT NULL,
    PRIMARY KEY (name, id)
);
