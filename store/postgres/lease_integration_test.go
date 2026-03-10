package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asger-noer/pebbles/store/postgres"
	"github.com/google/uuid"
)

// TestLeaseExcludesSecondHolder is the property the type exists for: while one
// process runs a projection, a second must be told to stand down rather than
// drive the same checkpoint concurrently.
func TestLeaseExcludesSecondHolder(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	name := "lease-test-" + uuid.NewString()

	holding := make(chan struct{})
	release := make(chan struct{})
	held := make(chan error, 1)
	go func() {
		held <- postgres.NewLease(pool, name).Hold(ctx, func(context.Context) error {
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	err := postgres.NewLease(pool, name).Hold(ctx, func(context.Context) error {
		t.Error("second holder ran while the lease was held")
		return nil
	})
	if !errors.Is(err, postgres.ErrLeaseHeld) {
		t.Fatalf("second Hold returned %v, want ErrLeaseHeld", err)
	}

	close(release)
	if err := <-held; err != nil {
		t.Fatalf("first Hold: %v", err)
	}

	// Once released the lease is available again, which is what lets a surviving
	// replica take over after the holder exits.
	ran := false
	if err := postgres.NewLease(pool, name).Hold(ctx, func(context.Context) error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("Hold after release: %v", err)
	}
	if !ran {
		t.Error("lease was not re-acquirable after release")
	}
}

// TestLeaseUnlocksReusedConnection guards the explicit pg_advisory_unlock: the
// lock is session-scoped and the connection returns to the pool still open, so
// without the unlock the next holder would block behind a lock nobody wants.
// Sequentially acquiring more times than the pool has connections forces reuse.
func TestLeaseUnlocksReusedConnection(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)
	name := "lease-reuse-" + uuid.NewString()

	for i := range 10 {
		if err := postgres.NewLease(pool, name).Hold(ctx, func(context.Context) error {
			return nil
		}); err != nil {
			t.Fatalf("Hold %d: %v", i, err)
		}
	}
}

// TestLeaseDifferentNamesDoNotCollide confirms projections are guarded
// independently — all seven of tipos's run at once, so a shared key would leave
// six of them permanently idle.
func TestLeaseDifferentNamesDoNotCollide(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	first := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- postgres.NewLease(pool, "lease-a-"+uuid.NewString()).Hold(ctx, func(context.Context) error {
			close(first)
			<-release
			return nil
		})
	}()
	<-first

	if err := postgres.NewLease(pool, "lease-b-"+uuid.NewString()).Hold(ctx, func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Hold on a different name: %v", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first Hold: %v", err)
	}
}

// TestLeasePropagatesCancellation checks that cancelling the outer context stops
// the held work and unwinds cleanly, since that is the normal shutdown path.
func TestLeasePropagatesCancellation(t *testing.T) {
	pool := newTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())

	running := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	var got error
	go func() {
		defer wg.Done()
		got = postgres.NewLease(pool, "lease-cancel-"+uuid.NewString()).Hold(ctx, func(runCtx context.Context) error {
			close(running)
			<-runCtx.Done()
			return nil
		})
	}()

	<-running
	cancel()

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("Hold did not return after the context was cancelled")
	}
	if got != nil {
		t.Fatalf("Hold returned %v, want nil", got)
	}
}

// TestLeaseReturnsFnError confirms a projection's own failure reaches the
// supervisor unchanged, rather than being masked as a lease problem.
func TestLeaseReturnsFnError(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	want := errors.New("projection exploded")
	got := postgres.NewLease(pool, "lease-err-"+uuid.NewString()).Hold(ctx, func(context.Context) error {
		return want
	})
	if !errors.Is(got, want) {
		t.Fatalf("Hold returned %v, want %v", got, want)
	}
}
