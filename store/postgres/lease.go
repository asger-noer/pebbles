package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// ErrLeaseHeld reports that another process already holds a projection's lease,
// so this one must not run it. It is an expected outcome, not a failure: it is
// how a multi-replica deployment settles on one writer per projection.
var ErrLeaseHeld = errors.New("pebbles/postgres: projection lease held by another process")

// ErrLeaseLost reports that the lease's connection dropped while the projection
// was running, so the lock is no longer held and the work was stopped. The
// caller should retry, which will either re-acquire the lease or find that
// another process has taken it.
var ErrLeaseLost = errors.New("pebbles/postgres: projection lease lost")

// leaseNamespace is the high half of every lease key: "pebb" in ASCII. Advisory
// locks share one namespace per database, so mixing in a constant keeps pebbles'
// keys from colliding with an application's own advisory locks.
const leaseNamespace = 0x70656262

// Lease is a single-writer guard for one named projection, built on a
// session-level Postgres advisory lock.
//
// It exists because a projection is not safe to run concurrently: two replicas
// driving one checkpoint row would each read from the other's committed
// position, so events would be applied twice, out of order, or skipped
// entirely. Postgres is already the coordination point, so no separate lease
// table or external lock service is needed.
//
// A Lease is inert until [Lease.Hold] is called and holds no resources between
// calls, so one can be reused across retries.
type Lease struct {
	pool      *pgxpool.Pool
	name      string
	key       int64
	heartbeat time.Duration
}

// NewLease returns a Lease guarding the projection called name. Use the
// projection's checkpoint key, so the lease and the cursor it protects are
// named by the same thing.
func NewLease(pool *pgxpool.Pool, name string) *Lease {
	return &Lease{pool: pool, name: name, key: leaseKey(name), heartbeat: 5 * time.Second}
}

// leaseKey maps a projection name onto an advisory lock key, namespaced to
// pebbles in the high 32 bits.
func leaseKey(name string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return leaseNamespace<<32 | int64(h.Sum32())
}

// Hold acquires the lease and runs fn while holding it, returning fn's error.
//
// It returns [ErrLeaseHeld] without calling fn if another process owns the
// lease, and [ErrLeaseLost] if the lease's connection dropped mid-run — in that
// case fn's context is cancelled first, so a projection stops rather than
// carrying on unguarded. Both are retryable.
//
// Hold occupies one connection from the pool for its whole duration, on top of
// whatever fn uses, so size the pool for one connection per concurrently held
// lease.
func (l *Lease) Hold(ctx context.Context, fn func(context.Context) error) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection for lease %q: %w", l.name, err)
	}
	defer conn.Release()

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", l.key).Scan(&acquired); err != nil {
		return fmt.Errorf("acquiring lease %q: %w", l.name, err)
	}
	if !acquired {
		return ErrLeaseHeld
	}

	// The lock is session-scoped, so releasing the connection would drop it.
	// Unlock explicitly anyway: the connection goes back to the pool for reuse,
	// where a lingering lock would block the next holder until the pool happened
	// to close it.
	defer func() {
		// A fresh context: ctx is typically already cancelled by the time we get
		// here, and skipping the unlock is exactly what we are avoiding.
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", l.key)
	}()

	// The heartbeat and fn run as a group so that whichever fails first cancels
	// the other and its error is the one reported. That ordering is the point: if
	// the heartbeat loses the connection, fn's own "context canceled" is a
	// consequence we caused, and the lost lease is the real story.
	g, runCtx := errgroup.WithContext(ctx)
	runCtx, cancel := context.WithCancel(runCtx)
	defer cancel()

	// Watch the connection. If it dies the lock is gone with it, and a projection
	// that kept running would be exactly the unguarded second writer this type
	// exists to prevent.
	g.Go(func() error {
		ticker := time.NewTicker(l.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return nil
			case <-ticker.C:
				if err := conn.Ping(runCtx); err != nil {
					if runCtx.Err() != nil {
						return nil // shutting down, not a lost lease
					}
					return fmt.Errorf("%w %q: %v", ErrLeaseLost, l.name, err)
				}
			}
		}
	})

	// cancel on return stops the heartbeat, which otherwise runs until runCtx is
	// done — and Wait would not return until it did.
	g.Go(func() error {
		defer cancel()
		return fn(runCtx)
	})

	return g.Wait()
}
