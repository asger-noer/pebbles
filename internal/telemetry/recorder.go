package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	metricOperationsDuration = "pebbles.operation.duration"
	metricOperationErrors    = "pebbles.operation.errors"
)

// Recorder records operation spans, durations, and errors.
type Recorder struct {
	tracer   trace.Tracer
	duration metric.Float64Histogram
	errors   metric.Int64Counter
}

// New creates a recorder with no-op instruments and providers unless supplied.
func New(scope string, tp trace.TracerProvider, mp metric.MeterProvider) Recorder {
	noopMeter := metricnoop.NewMeterProvider().Meter(scope)
	duration, _ := noopMeter.Float64Histogram(metricOperationsDuration)
	errors, _ := noopMeter.Int64Counter(metricOperationErrors)
	recorder := Recorder{
		tracer:   tracenoop.NewTracerProvider().Tracer(scope),
		duration: duration,
		errors:   errors,
	}

	if tp != nil {
		recorder.tracer = tp.Tracer(scope)
	}
	if mp != nil {
		meter := mp.Meter(scope)
		if duration, err := meter.Float64Histogram(
			metricOperationsDuration,
			metric.WithDescription("Duration of Pebbles operations."),
			metric.WithUnit("s"),
		); err == nil {
			recorder.duration = duration
		}
		if errors, err := meter.Int64Counter(
			metricOperationErrors,
			metric.WithDescription("Number of failed Pebbles operations."),
			metric.WithUnit("count"),
		); err == nil {
			recorder.errors = errors
		}
	}

	return recorder
}

// Start starts an operation span and returns a function that records its result.
func (r Recorder) Start(ctx context.Context, operation string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	ctx, span := r.tracer.Start(ctx, operation, trace.WithAttributes(attrs...))
	started := time.Now()

	return ctx, func(err error) {
		outcome := "ok"
		if err != nil {
			outcome = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			r.errors.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation)))
		}

		r.duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("outcome", outcome),
		))
		span.End()
	}
}
