ALTER TABLE pebbles.event
    ADD COLUMN name TEXT;

UPDATE pebbles.event SET name = kind;

ALTER TABLE pebbles.event
    ALTER COLUMN name SET NOT NULL,
    DROP COLUMN kind,
    DROP COLUMN version,
    DROP COLUMN content_type;

---- create above / drop below ----

ALTER TABLE pebbles.event
    ADD COLUMN kind TEXT,
    ADD COLUMN version INTEGER,
    ADD COLUMN content_type TEXT;

UPDATE pebbles.event SET kind = name, version = 1, content_type = 'application/protobuf';

ALTER TABLE pebbles.event
    ALTER COLUMN kind SET NOT NULL,
    ALTER COLUMN version SET NOT NULL,
    ALTER COLUMN content_type SET NOT NULL,
    DROP COLUMN name;
