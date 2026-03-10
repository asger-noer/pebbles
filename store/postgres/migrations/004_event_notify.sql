-- Wake-up signal for projection runners. Postgres does not announce table
-- changes on its own, so an insert has to say so explicitly; a trigger is what
-- makes that automatic for every writer, including psql sessions, backfills and
-- any future non-Go client. Runners LISTEN on this channel (see Notifier) and
-- would otherwise only learn about a write on their next poll, leaving the read
-- models trailing every commit by up to one poll interval.
--
-- A trigger must EXECUTE FUNCTION, hence the wrapper — Postgres has no way to
-- attach a bare NOTIFY to a table.
--
-- FOR EACH STATEMENT, not FOR EACH ROW: appending N events in one statement
-- should signal once, not N times. Postgres also collapses duplicate
-- (channel, payload) notifications within a transaction, so the empty payload
-- means a multi-statement transaction — the usual case, one statement per event
-- — still yields a single wake-up.
--
-- Delivery is at COMMIT, which is the correct edge on both sides: a rolled-back
-- append announces nothing, and an uncommitted event is invisible to
-- ReadAllEvents anyway. The signal is a hint, never a guarantee — a woken runner
-- may still read nothing, because ReadAllEvents withholds rows until the
-- pg_snapshot_xmin horizon passes their xact_id. Polling stays the backstop.
CREATE FUNCTION pebbles.notify_event() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('pebbles_event', '');
    RETURN NULL;
END;
$$;

CREATE TRIGGER event_notify
    AFTER INSERT ON pebbles.event FOR EACH STATEMENT
    EXECUTE FUNCTION pebbles.notify_event();

---- create above / drop below ----

DROP TRIGGER event_notify ON pebbles.event;
DROP FUNCTION pebbles.notify_event();
