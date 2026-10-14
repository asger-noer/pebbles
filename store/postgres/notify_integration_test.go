package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	customerv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/customer/v1"
	"github.com/asger-noer/pebbles/projection"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

// waitForWakeup reports whether a wake-up arrived within a generous timeout.
// The point of the notification is that it beats any poll interval, so the
// timeout only needs to be short enough to fail if nothing is delivered at all.
func waitForWakeup(t *testing.T, ch <-chan struct{}) bool {
	t.Helper()
	select {
	case <-ch:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// drain clears any wake-up already pending, so a later assertion cannot pass on
// a stale signal (notably the one Notifier broadcasts on connect).
func drain(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// newNotifyFixture starts a Notifier with one subscriber and returns a
// repository writing to it, with the connect-time broadcast already drained.
func newNotifyFixture(t *testing.T) (*stream.Repository[struct{}, pebbles.Event], <-chan struct{}, *store.Store) {
	t.Helper()
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	notifier := postgres.NewNotifier(pool)
	wake := notifier.Subscribe()

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := notifier.Run(runCtx); err != nil {
			t.Errorf("notifier.Run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// Notifier broadcasts once on connect; wait for it so we know LISTEN is
	// established before any append, then clear it.
	if !waitForWakeup(t, wake) {
		t.Fatal("no connect-time broadcast; notifier never established LISTEN")
	}
	drain(wake)

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	return stream.NewRepository(customerCodecs, st), wake, st
}

// TestNotifyOnAppend proves a committed append wakes a listener, which lets a
// projection runner react to a write instead of waiting out its poll interval.
func TestNotifyOnAppend(t *testing.T) {
	ctx := context.Background()
	customers, wake, _ := newNotifyFixture(t)

	if err := customers.Append(ctx, uuid.New(), &customerv1.CustomerNameUpdated{Name: "Ada"}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	if !waitForWakeup(t, wake) {
		t.Fatal("append did not deliver a wake-up")
	}
}

// TestNotifyFanOut proves one Notifier serves several subscribers, which is
// what lets all seven projections share a single listening connection.
func TestNotifyFanOut(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	notifier := postgres.NewNotifier(pool)
	subs := []<-chan struct{}{notifier.Subscribe(), notifier.Subscribe(), notifier.Subscribe()}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := notifier.Run(runCtx); err != nil {
			t.Errorf("notifier.Run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	for i, ch := range subs {
		if !waitForWakeup(t, ch) {
			t.Fatalf("subscriber %d got no connect-time broadcast", i)
		}
		drain(ch)
	}

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	customers := stream.NewRepository(customerCodecs, st)

	if err := customers.Append(ctx, uuid.New(), &customerv1.CustomerNameUpdated{Name: "Barbara"}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	for i, ch := range subs {
		if !waitForWakeup(t, ch) {
			t.Fatalf("subscriber %d got no wake-up", i)
		}
	}
}

// countingHandler records events into a channel so a test can observe a
// projection applying them.
type countingHandler struct {
	name    string
	applied chan uuid.UUID
}

func (h *countingHandler) Name() string { return h.name }

func (h *countingHandler) Subscriptions() pebbles.Subscriptions {
	return pebbles.Subscribe(customerAggregate{}, &customerv1.CustomerNameUpdated{})
}

func (h *countingHandler) Handle(_ context.Context, p pebbles.Pebble[pebbles.Event]) error {
	select {
	case h.applied <- p.StreamID:
	default:
	}
	return nil
}

// TestWakeupBeatsPollInterval is the point of the whole mechanism: with a poll
// interval far longer than the test's patience, a runner still applies a fresh
// append promptly, because the append's NOTIFY wakes it. Without WithWakeup this
// test could only pass by waiting out the interval.
func TestWakeupBeatsPollInterval(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	notifier := postgres.NewNotifier(pool)
	wake := notifier.Subscribe()

	runCtx, cancel := context.WithCancel(ctx)
	notifierDone := make(chan struct{})
	go func() {
		defer close(notifierDone)
		if err := notifier.Run(runCtx); err != nil {
			t.Errorf("notifier.Run: %v", err)
		}
	}()

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)

	// A unique checkpoint name per run, so the runner starts from wherever the
	// log already is rather than replaying another test's events.
	handler := &countingHandler{
		name:    "notify-test-" + uuid.NewString(),
		applied: make(chan uuid.UUID, 16),
	}
	pos, err := postgres.NewCheckpointStore(engine).Load(ctx, handler.Name())
	if err != nil {
		t.Fatalf("loading checkpoint: %v", err)
	}
	if pos != 0 {
		t.Fatalf("want a fresh checkpoint, got position %d", pos)
	}

	runner := projection.NewRunner(st, postgres.NewCheckpointStore(engine), handler,
		[]codec.Registrar{customerCodecs},
		// Deliberately longer than any timeout below: if the wake-up does not
		// work, this test fails rather than passing slowly.
		projection.WithPollInterval(2*time.Minute),
		projection.WithWakeup(wake),
	)
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		if err := runner.Run(runCtx); err != nil {
			t.Errorf("runner.Run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-runnerDone
		<-notifierDone
	})

	// Let the runner reach its first idle wait before appending, so the append's
	// notification is what wakes it rather than its startup catch-up.
	time.Sleep(500 * time.Millisecond)
	drainIDs(handler.applied)

	streamID := uuid.New()
	customers := stream.NewRepository(customerCodecs, st)
	if err := customers.Append(ctx, streamID, &customerv1.CustomerNameUpdated{Name: "Katherine"}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	// Generous relative to the wake-up path (milliseconds), tiny relative to the
	// 2 minute poll interval it is proving we did not wait for.
	select {
	case got := <-handler.applied:
		if got != streamID {
			t.Fatalf("applied stream %s, want %s", got, streamID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not apply the append within 10s; wake-up did not fire")
	}
}

// drain clears pending stream IDs from a countingHandler channel.
func drainIDs(ch chan uuid.UUID) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// TestSubscribeAfterRunPanics guards the ordering requirement in main-style
// wiring: every runner must subscribe before the notifier starts, or it would
// silently never be woken.
func TestSubscribeAfterRunPanics(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	notifier := postgres.NewNotifier(pool)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = notifier.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// Give Run time to mark itself started.
	time.Sleep(250 * time.Millisecond)

	defer func() {
		if recover() == nil {
			t.Fatal("want panic when subscribing after Run, got none")
		}
	}()
	notifier.Subscribe()
}
