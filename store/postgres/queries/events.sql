-- name: AppendEvent :exec
INSERT INTO pebbles.event (id, stream_type, stream_id, sequence, name, at, headers, data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetEvent :one
SELECT id, stream_type, stream_id, sequence, name, at, headers, data
FROM pebbles.event
WHERE id = $1;

-- name: ListStreamEvents :many
SELECT id, stream_type, stream_id, sequence, name, at, headers, data
FROM pebbles.event
WHERE stream_type = $1 AND stream_id = $2 AND sequence >= $3
ORDER BY sequence ASC;

-- name: LastSequence :one
SELECT COALESCE(MAX(sequence), 0)::bigint
FROM pebbles.event
WHERE stream_type = $1 AND stream_id = $2;

-- name: ListStreams :many
SELECT DISTINCT stream_id
FROM pebbles.event
WHERE stream_type = $1
ORDER BY stream_id;

-- name: ReadAllEvents :many
-- Cross-stream read in global-position order. The xact_id horizon excludes rows
-- whose writing transaction may still be in flight, so advancing the cursor by
-- global_position can never skip a low position that commits late.
SELECT id, stream_type, stream_id, sequence, name, at, headers, data, global_position
FROM pebbles.event
WHERE global_position > @from_position
  AND xact_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY global_position ASC
LIMIT @max_events;

-- name: LoadCheckpoint :one
SELECT position FROM pebbles.projection_checkpoint WHERE name = $1;

-- name: SaveCheckpoint :exec
INSERT INTO pebbles.projection_checkpoint (name, position)
VALUES ($1, $2)
ON CONFLICT (name) DO UPDATE SET position = EXCLUDED.position;

-- name: LoadKeyedSnapshot :one
SELECT model FROM pebbles.projection_keyed_snapshot WHERE name = $1 AND key = $2;

-- name: SaveKeyedSnapshot :exec
INSERT INTO pebbles.projection_keyed_snapshot (name, key, model)
VALUES ($1, $2, $3)
ON CONFLICT (name, key) DO UPDATE SET model = EXCLUDED.model;

-- name: LoadKeyedIndex :many
SELECT id, key FROM pebbles.projection_keyed_index WHERE name = $1;

-- name: SaveKeyedIndex :exec
INSERT INTO pebbles.projection_keyed_index (name, id, key)
VALUES ($1, $2, $3)
ON CONFLICT (name, id) DO UPDATE SET key = EXCLUDED.key;

-- name: GetStreamKey :one
SELECT key FROM pebbles.stream_key WHERE stream_id = $1;

-- name: CreateStreamKey :exec
INSERT INTO pebbles.stream_key (stream_id, key)
VALUES ($1, $2)
ON CONFLICT (stream_id) DO NOTHING;

-- name: DeleteStreamKey :exec
DELETE FROM pebbles.stream_key WHERE stream_id = $1;

-- name: DeleteKeyedSnapshotsByKey :exec
DELETE FROM pebbles.projection_keyed_snapshot WHERE name = $1 AND key = $2;
