-- Per-stream AES-256 encryption keys. The key material is stored as raw bytes;
-- production deployments should wrap it with a KMS envelope key.
CREATE TABLE pebbles.stream_key (
    stream_id UUID PRIMARY KEY,
    key       BYTEA NOT NULL
);

---- create above / drop below ----

DROP TABLE pebbles.stream_key;
