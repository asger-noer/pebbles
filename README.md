# Pebbles

Pebbles is an opinionated event sourcing framework for Go.

## Design Choices

### Event format

Events are protobuf messages. Identity comes from the message's fully-qualified
descriptor name (e.g. `myapp.order.v1.OrderPlaced`) — there is no separate
event name or version field to keep in sync with the schema. Use `buf` to
generate Go types from `.proto` files (see `buf.yaml` / `buf.gen.yaml`); an
event type is any generated message registered with a `codec.Registry`.

### Event storage

The default implementation uses PostgreSQL or in-memorry, but other databases can be used by implementing the storage interfaces.

### Event versioning

Events are immutable, and the source of truth for the state of the system. They are stored in an append-only log, and can be replayed to reconstruct the state of the system at any point in time. Schema evolution happens by adding a new message and registering an upcaster (`codec.RegisterUpcaster`) from the old message to the new one, keyed by message name — old events are never rewritten, only upcast at read time.

### Consistency boundaries

`stream.Repository.Append`, `.Delete`, and `.Execute` do not accept caller
transactions. Pebbles commits each event batch to one stream atomically.
`Repository.Execute` also checks that its stream has not changed since it loaded
state. Writes to different streams commit independently. Keep invariants that
need immediate enforcement in one aggregate.

For cross-stream rules, record the relevant facts or requests, then apply the
policy asynchronously. Use a `projection.Handler` and `projection.Runner` as
the event consumer. Consumers receive events at least once, so make their
outputs idempotent. Model pending outcomes and, when needed, compensating events
instead of coordinating writes across aggregates.

## Folding events into state

Everything in Pebbles is a fold over events — the difference between the
abstractions is *which* events are folded and *how* the result is kept. They all
share the same shape (`Initial()` + `Apply(state, event)`); pick the smallest one
that fits.

| Construct | Reads | Maintained? | Read via | Reach for it when |
|---|---|---|---|---|
| `stream.Repository` (`pebbles.Aggregate[S, E]`) | one stream | no — folded on demand | `Load(streamID)` | write-side aggregate state for command handling |
| `stream.View` (`pebbles.Stream[M, E]`) | one stream | no — folded on demand | `Load(streamID)` | a read-model *view* of one stream, computed per read |
| `projection.Keyed` (`pebbles.KeyedProjector` / `RoutedProjector`) | many streams (`Subscriptions`) | yes — caught up & snapshotted | `Get(key)` | one read model **per entity/key** (e.g. per‑customer) |
| `projection.Handler` + `projection.Runner` | many streams (`Subscriptions`) | yes — cursor only | your own store | external/queryable read models (a SQL view), or events that don't share a sum interface |

Notes:

- **One stream vs many.** `Stream` and `Aggregate` fold a single stream you
  address by id. `Keyed` and `Handler` fold across many streams, bounded by a
  `Subscriptions` binding (build it from zero values with `pebbles.Subscribe`).
- **On demand vs maintained.** `Load` re-folds the stream on every call (no cursor,
  no snapshot). `Keyed` and `Handler` are driven by a background `Run` that catches
  up over the global log and persists progress so they can resume.
- **Keyed resolution.** `Keyed` keeps one model per key and learns an index
  (`streamID → key`) as it goes, so events that don't carry the key (e.g. an
  `ItemAdded` after `OrderPlaced`) are still routed correctly. Use `KeyedProjector`
  (managed `Key`) for the common case, or `RoutedProjector` (explicit `Route`) when
  a key must be resolved across streams from a payload field.
- **Typed fold vs raw envelope.** `Keyed` folds typed events over a
  sum interface `E`; drop to a `Handler` when events don't share one or you need the
  raw `pebbles.Pebble` (metadata, headers).
