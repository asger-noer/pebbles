package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewDoesNotUseGlobalProvidersWhenOmitted(t *testing.T) {
	ctx := context.Background()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { _ = tracerProvider.Shutdown(ctx) })
	otel.SetTracerProvider(tracerProvider)

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = meterProvider.Shutdown(ctx) })
	otel.SetMeterProvider(meterProvider)

	recorder := New("test/telemetry", nil, nil)
	ctx, finish := recorder.Start(ctx, "test.operation")
	finish(nil)

	if spans := spanRecorder.Ended(); len(spans) != 0 {
		t.Fatalf("want no spans from global provider, got %d", len(spans))
	}

	rm := &metricdata.ResourceMetrics{}
	if err := reader.Collect(ctx, rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) != 0 {
		t.Fatalf("want no metrics from global provider, got %d scopes", len(rm.ScopeMetrics))
	}
}
