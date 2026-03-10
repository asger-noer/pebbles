package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel is the Postgres channel carrying "the event log may have grown"
// wake-ups. The insert trigger on pebbles.event signals it (see migration
// 004_event_notify.sql); [Notifier] listens on it.
const NotifyChannel = "pebbles_event"

// Notifier turns Postgres NOTIFY signals from the pebbles.event insert trigger
// into wake-ups for projection runners, so a runner reacts to a commit instead
// of waiting out its poll interval.
//
// One Notifier serves any number of runners: call [Notifier.Subscribe] once per
// runner before [Notifier.Run], and pass each channel to
// projection.WithWakeup. Run holds one connection from the pool for as long as
// it is listening, so size the pool for it.
//
// A wake-up is a hint, not a delivery: it says "the log may have grown", never
// "these events are now readable". Runners must keep polling as a backstop —
// see the migration comment on the trigger for why a woken read can come back
// empty.
type Notifier struct {
	pool     *pgxpool.Pool
	backoff  time.Duration
	mu       sync.Mutex
	subs     []chan struct{}
	listened bool
}

// NewNotifier returns a Notifier over pool. It does not connect until Run.
func NewNotifier(pool *pgxpool.Pool) *Notifier {
	return &Notifier{pool: pool, backoff: time.Second}
}

// Subscribe registers a new wake-up channel and returns it. It must be called
// before [Notifier.Run]; subscribing afterwards panics, since a subscriber
// added mid-run could silently miss the initial catch-up signal.
//
// The channel is buffered with room for one pending wake-up and is never
// closed. Sends are non-blocking and drop when that slot is already full: a
// runner that has not yet acted on the previous wake-up will read the new
// events too, so coalescing them loses nothing.
func (n *Notifier) Subscribe() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listened {
		panic("pebbles/postgres: Notifier.Subscribe called after Run")
	}
	ch := make(chan struct{}, 1)
	n.subs = append(n.subs, ch)
	return ch
}

// Run listens for notifications and broadcasts them to every subscriber until
// ctx is cancelled, returning nil on cancellation. A dropped connection is
// retried with a fixed backoff rather than being reported, since losing the
// listener degrades latency but not correctness.
func (n *Notifier) Run(ctx context.Context) error {
	n.mu.Lock()
	n.listened = true
	n.mu.Unlock()

	for {
		if err := n.listen(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Reconnect. Nothing to report: the runners' poll interval covers
			// the gap, so this costs latency, not events.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(n.backoff):
			}
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// listen holds one connection for the duration of a single LISTEN session,
// returning when it fails or ctx is cancelled.
func (n *Notifier) listen(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return err
	}

	// Broadcast once on (re)connect: events may have been appended while we had
	// no listener, and those notifications are gone. This also gives every
	// runner an immediate first catch-up rather than one poll interval of
	// unnecessary lag at startup.
	n.broadcast()

	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("waiting for notification: %w", err)
		}
		n.broadcast()
	}
}

// broadcast signals every subscriber, skipping any that already has a wake-up
// pending.
func (n *Notifier) broadcast() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ch := range n.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
