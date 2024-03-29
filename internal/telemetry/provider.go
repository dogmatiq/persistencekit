package telemetry

import (
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Provider provides Recorder instances scoped to particular subsystems.
type Provider struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
	Attrs          []Attr
}

// Recorder returns a new Recorder instance.
//
// pkg is the path to the Go package that is performing the instrumentation. If
// it is an internal package, use the package path of the public parent package
// instead.
//
// name is the one-word name of the subsystem that the recorder is for, for
// example "journal" or "aggregate".
func (p *Provider) Recorder(pkg, name string, attrs ...Attr) *Recorder {
	r := &Recorder{
		name: "io.dogmatiq.persistencekit." + name,
		attrs: append(
			slices.Clone(p.Attrs),
			attrs...,
		),
		tracer: p.TracerProvider.Tracer(pkg, tracerVersion),
		meter:  p.MeterProvider.Meter(pkg, meterVersion),
		logger: p.Logger,
	}

	r.errors = r.Int64Counter(
		"errors",
		metric.WithDescription("The number of errors that have occurred."),
		metric.WithUnit("{error}"),
	)

	return r
}
