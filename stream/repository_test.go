package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type orderAggregate struct{}
type transitionAggregate struct{}

func (orderAggregate) StreamType() string { return "order" }
func (orderAggregate) Initial() Summary   { return Summary{} }
func (orderAggregate) Apply(s Summary, e pebbles.Event) Summary {
	if c, ok := e.(*orderv2.ItemAdded); ok {
		s.Count++
		s.Total += int(c.GetPriceCents())
	}
	return s
}

func (transitionAggregate) StreamType() string { return "transition" }
func (transitionAggregate) Initial() bool      { return false }
func (transitionAggregate) Apply(_ bool, _ pebbles.Event) bool {
	return true
}

func newOrderRegistry(t *testing.T) *codec.Registry[orderAggregate] {
	t.Helper()
	r := codec.NewRegistry[orderAggregate]()
	codec.Register[*orderv2.ItemAdded](r)
	return r
}

func newRepo(t *testing.T, s *store.Store) *stream.Repository[Summary, pebbles.Event] {
	t.Helper()
	return stream.NewRepository(newOrderRegistry(t), s)
}

func TestRepositoryTelemetryRecordsSpansDurationsAndErrors(t *testing.T) {
	ctx := context.Background()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { _ = tracerProvider.Shutdown(ctx) })

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = meterProvider.Shutdown(ctx) })

	st := store.New(memory.New())
	repo := stream.NewRepository(
		newOrderRegistry(t),
		st,
		stream.WithTraceProvider(tracerProvider),
		stream.WithMetricsProvider(meterProvider),
	)
	id := uuid.New()
	if err := repo.Append(ctx, id, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Load(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Load(ctx, id); !errors.Is(err, pebbles.ErrDeleted) {
		t.Fatalf("want ErrDeleted, got %v", err)
	}

	wantSpans := map[string]bool{
		"stream.repository.append": false,
		"stream.repository.load":   false,
		"stream.repository.delete": false,
	}
	var failedLoad bool
	for _, span := range spanRecorder.Ended() {
		if _, ok := wantSpans[span.Name()]; ok {
			wantSpans[span.Name()] = true
		}
		if span.Name() == "stream.repository.load" && span.Status().Code == codes.Error {
			failedLoad = true
		}
	}
	for name, found := range wantSpans {
		if !found {
			t.Errorf("missing span %q", name)
		}
	}
	if !failedLoad {
		t.Error("expected failed Load span to have error status")
	}

	rm := &metricdata.ResourceMetrics{}
	if err := reader.Collect(ctx, rm); err != nil {
		t.Fatal(err)
	}
	var durationCount, errorCount uint64
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch data := metric.Data.(type) {
			case metricdata.Histogram[float64]:
				if metric.Name == "pebbles.operation.duration" {
					for _, point := range data.DataPoints {
						durationCount += point.Count
					}
				}
			case metricdata.Sum[int64]:
				if metric.Name == "pebbles.operation.errors" {
					for _, point := range data.DataPoints {
						errorCount += uint64(point.Value)
					}
				}
			}
		}
	}
	if durationCount != 4 {
		t.Errorf("want 4 operation durations, got %d", durationCount)
	}
	if errorCount != 1 {
		t.Errorf("want 1 operation error, got %d", errorCount)
	}
}

func TestRepositoryAppendAndLoad(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, id,
		&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10},
		&orderv2.ItemAdded{Name: "Mouse", PriceCents: 5},
	); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got != (Summary{Count: 2, Total: 15}) {
		t.Fatalf("want {2 15}, got %+v", got)
	}
}

func TestRepositoryDeleteReturnsErrDeleted(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, id, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}

	_, err := repo.Load(ctx, id)
	if !errors.Is(err, pebbles.ErrDeleted) {
		t.Fatalf("want ErrDeleted, got %v", err)
	}
}

func TestRepositoryExecuteRejectsStaleTransition(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	codecs := codec.NewRegistry[transitionAggregate]()
	codec.Register[*orderv2.ItemAdded](codecs)
	repo := stream.NewRepository(codecs, st)
	id := uuid.New()

	loaded := make(chan struct{})
	resume := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := repo.Execute(ctx, id, func(context.Context, bool) ([]pebbles.Event, error) {
			close(loaded)
			<-resume
			return []pebbles.Event{&orderv2.ItemAdded{Name: "completion"}}, nil
		})
		result <- err
	}()

	<-loaded
	if _, err := repo.Execute(ctx, id, func(_ context.Context, complete bool) ([]pebbles.Event, error) {
		if complete {
			t.Fatal("expected incomplete state")
		}
		return []pebbles.Event{&orderv2.ItemAdded{Name: "completion"}}, nil
	}); err != nil {
		t.Fatalf("executing first transition: %v", err)
	}
	close(resume)

	if err := <-result; !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want stale transition to return ErrConflict, got %v", err)
	}
	events, err := st.Read(ctx, "transition", id, 0)
	if err != nil {
		t.Fatalf("reading events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want exactly one completion event, got %d", len(events))
	}
}

func TestRepositoryExecuteReturnsErrDeletedOnDeletedStream(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, id, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}

	_, err := repo.Execute(ctx, id, func(context.Context, Summary) ([]pebbles.Event, error) {
		return []pebbles.Event{&orderv2.ItemAdded{Name: "Mouse", PriceCents: 5}}, nil
	})
	if !errors.Is(err, pebbles.ErrDeleted) {
		t.Fatalf("want ErrDeleted, got %v", err)
	}
}
