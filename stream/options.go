package stream

import (
	"github.com/asger-noer/pebbles/internal/telemetry"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	traceProvider   trace.TracerProvider
	metricsProvider metric.MeterProvider
	recorder        telemetry.Recorder
}

// Option configures a Repository or View.
type Option func(*options)

// WithTraceProvider sets the OpenTelemetry provider used for spans.
func WithTraceProvider(provider trace.TracerProvider) Option {
	return func(o *options) { o.traceProvider = provider }
}

// WithMetricsProvider sets the OpenTelemetry provider used for metrics.
func WithMetricsProvider(provider metric.MeterProvider) Option {
	return func(o *options) { o.metricsProvider = provider }
}

func resolveOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	o.recorder = telemetry.New("github.com/asger-noer/pebbles/stream", o.traceProvider, o.metricsProvider)
	return o
}
