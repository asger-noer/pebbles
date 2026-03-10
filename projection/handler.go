// Projection drives read models built by folding events across many
// streams. It offers two layers:
//
//   - Handler + Runner: a low-level primitive that pushes each hydrated event
//     into a Handler which owns its own (typically external) read model, with
//     the cursor persisted via a CheckpointStore. Use it when events do not
//     share a sum interface or you need the raw Pebble envelope.
//   - Projection[M, E]: a generic, Repository-shaped wrapper around a
//     pebbles.Projector[M, E] that folds typed events into an in-memory model
//     and snapshots it via a SnapshotStore.
//
// Both are bounded: a projection must declare a non-empty Subscriptions map and
// only ever sees the (stream type, kind) pairs it lists. Both share one driver
// loop that reads the log in commit-safe global-position order, applies the
// binding, and advances the cursor.
package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	"github.com/asger-noer/pebbles/store"
)

// Handler consumes a bounded set of events across streams. It owns its read
// model (a SQL view, a cache, ...) and its idempotency: a Runner delivers each
// event at least once. Name is the checkpoint key.
type Handler interface {
	Name() string
	Subscriptions() pebbles.Subscriptions
	Handle(ctx context.Context, p pebbles.Pebble[pebbles.Event]) error
}

// ErrorHandler decides what happens when applying an event fails. Returning nil
// skips the event and continues; returning an error stops the run with it.
type ErrorHandler func(p pebbles.Pebble[pebbles.Event], err error) error

type options struct {
	batchSize    int
	pollInterval time.Duration
	onError      ErrorHandler
	wakeup       <-chan struct{}
}

func defaultOptions() options {
	return options{batchSize: 100, pollInterval: time.Second}
}

// Option configures a Runner or Projection.
type Option func(*options)

// WithBatchSize sets how many events are read per ReadAll call (default 100).
func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithPollInterval sets how long the driver waits after catching up before
// polling again (default 1s).
func WithPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.pollInterval = d
		}
	}
}

// WithErrorHandler installs an ErrorHandler. The default stops the run on the
// first apply error.
func WithErrorHandler(fn ErrorHandler) Option {
	return func(o *options) { o.onError = fn }
}

// WithWakeup supplies a channel that signals "the log may have grown", so a
// caught-up run reacts to a commit instead of waiting out its poll interval —
// see postgres.Notifier for the Postgres NOTIFY implementation.
//
// It shortens the idle wait; it does not replace it. A wake-up carries no
// guarantee that anything is readable yet (ReadAll withholds events until the
// commit-safety horizon passes them), so the poll interval stays in place as
// the backstop and remains the bound on worst-case lag.
func WithWakeup(ch <-chan struct{}) Option {
	return func(o *options) { o.wakeup = ch }
}

func resolveOptions(opts []Option) options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// driver is the shared catch-up loop behind both Runner and Projection. It owns
// position tracking, the gap-safe read, hydration and the binding filter, and
// delegates the projection-specific work to its load/apply/commit hooks.
type driver struct {
	store      *store.Store
	registries map[string]codec.Registrar
	subs       pebbles.Subscriptions
	opts       options

	load   func(ctx context.Context) (int64, error)
	apply  func(ctx context.Context, p pebbles.Pebble[pebbles.Event]) error
	commit func(ctx context.Context, position int64) error
}

// run blocks until ctx is cancelled (returning nil) or an unrecoverable error
// occurs. It reads the log in batches from the loaded position, applies each
// in-binding event, and commits the advanced position once per batch.
func (d *driver) run(ctx context.Context) error {
	pos, err := d.load(ctx)
	if err != nil {
		if cancelled(ctx, err) {
			return nil
		}
		return fmt.Errorf("loading start position: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		batch, err := d.store.ReadAll(ctx, pos, d.opts.batchSize)
		if err != nil {
			if cancelled(ctx, err) {
				return nil
			}
			return fmt.Errorf("reading events: %w", err)
		}

		if len(batch) == 0 {
			d.idle(ctx)
			continue
		}

		for _, rec := range batch {
			pos = rec.Position // advance even past events outside the binding

			// Deleted tombstones bypass hydration — they carry no data and no
			// codec. Pass them through to any projection that subscribes to their
			// stream type so it can clean up its read model.
			if rec.Event.Name == pebbles.DeletedName {
				if _, subscribed := d.subs[rec.Event.StreamType]; subscribed {
					pebble := pebbles.Pebble[pebbles.Event]{
						ID:         rec.Event.ID,
						StreamType: rec.Event.StreamType,
						StreamID:   rec.Event.StreamID,
						Sequence:   rec.Event.Sequence,
						At:         rec.Event.At,
						Headers:    rec.Event.Headers,
						Event:      &pebbles.Deleted{},
					}
					if err := d.apply(ctx, pebble); err != nil {
						if cancelled(ctx, err) {
							return nil
						}
						if d.opts.onError == nil {
							return err
						}
						if e := d.opts.onError(pebble, err); e != nil {
							return e
						}
					}
				}
				continue
			}

			if !d.subs.Includes(rec.Event.StreamType, rec.Event.Name) {
				continue
			}

			reg, ok := d.registries[rec.Event.StreamType]
			if !ok {
				return fmt.Errorf("hydrating event: no codec registry for stream type %q", rec.Event.StreamType)
			}
			payload, err := reg.Hydrate(rec.Event.Name, rec.Event.Data)
			if err != nil {
				return fmt.Errorf("hydrating event %s: %w", rec.Event.Name, err)
			}
			ev, ok := payload.(pebbles.Event)
			if !ok {
				return fmt.Errorf("hydrated %s as %T, which does not satisfy pebbles.Event", rec.Event.Name, payload)
			}

			pebble := pebbles.Pebble[pebbles.Event]{
				ID:         rec.Event.ID,
				StreamType: rec.Event.StreamType,
				StreamID:   rec.Event.StreamID,
				Sequence:   rec.Event.Sequence,
				At:         rec.Event.At,
				Headers:    rec.Event.Headers,
				Event:      ev,
			}

			if err := d.apply(ctx, pebble); err != nil {
				if cancelled(ctx, err) {
					return nil
				}
				if d.opts.onError == nil {
					return err
				}
				if e := d.opts.onError(pebble, err); e != nil {
					return e
				}
			}
		}

		if err := d.commit(ctx, pos); err != nil {
			if cancelled(ctx, err) {
				return nil
			}
			return fmt.Errorf("committing position %d: %w", pos, err)
		}
	}
}

// cancelled reports whether err is just the consequence of ctx being cancelled,
// so a shutdown mid-batch is a clean stop rather than a projection failure. It
// checks ctx as well as err because a driver hook may surface a cancellation as
// some transport-specific error that does not wrap context.Canceled.
func cancelled(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// idle waits after a read that found nothing, returning as soon as ctx is
// cancelled, a wake-up arrives, or the poll interval elapses — whichever comes
// first. The timer is deliberately kept even when a wake-up channel is
// configured: notifications can be missed (no listener during a reconnect) and
// a delivered one does not mean the event is readable yet, so polling stays the
// backstop that bounds worst-case lag.
func (d *driver) idle(ctx context.Context) {
	timer := time.NewTimer(d.opts.pollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-d.opts.wakeup: // nil channel blocks forever, leaving just the timer
	case <-timer.C:
	}
}

// Runner drives a Handler, persisting its cursor via a CheckpointStore.
type Runner struct {
	driver *driver
}

// buildRegistryMap builds a stream-type-keyed lookup from a slice of registries
// and validates that every subscribed stream type has a registry. Panics on
// missing coverage — misconfiguration that should fail at startup.
func buildRegistryMap(subs pebbles.Subscriptions, registries []codec.Registrar) map[string]codec.Registrar {
	m := make(map[string]codec.Registrar, len(registries))
	for _, r := range registries {
		m[r.StreamType()] = r
	}
	for streamType := range subs {
		if _, ok := m[streamType]; !ok {
			panic(fmt.Sprintf("projection: no codec registry provided for subscribed stream type %q", streamType))
		}
	}
	return m
}

// NewRunner wires a Handler to the event log. It panics if the handler's
// Subscriptions are invalid (empty, or a stream type with no kinds), or if any
// subscribed stream type has no corresponding registry — misconfiguration that
// should fail at startup.
func NewRunner(s *store.Store, cp store.CheckpointStore, h Handler, registries []codec.Registrar, opts ...Option) *Runner {
	subs := h.Subscriptions()
	if err := subs.Validate(); err != nil {
		panic(err)
	}
	name := h.Name()
	return &Runner{
		driver: &driver{
			store:      s,
			registries: buildRegistryMap(subs, registries),
			subs:       subs,
			opts:       resolveOptions(opts),
			load:       func(ctx context.Context) (int64, error) { return cp.Load(ctx, name) },
			apply:      func(ctx context.Context, p pebbles.Pebble[pebbles.Event]) error { return h.Handle(ctx, p) },
			commit:     func(ctx context.Context, position int64) error { return cp.Save(ctx, name, position) },
		},
	}
}

// Run blocks, driving the handler until ctx is cancelled or a fatal error
// occurs. It returns nil on context cancellation.
func (r *Runner) Run(ctx context.Context) error { return r.driver.run(ctx) }
