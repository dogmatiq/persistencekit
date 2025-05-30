package telemetry

import (
	"runtime/debug"

	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Provider provides Recorder instances scoped to particular subsystems.
type Provider struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	LoggerProvider log.LoggerProvider
}

// Recorder records traces, metrics and logs for a particular subsystem.
type Recorder struct {
	tracer trace.Tracer
	meter  metric.Meter
	logger log.Logger

	errorCount              Instrument[int64]
	operationCount          Instrument[int64]
	operationsInFlightCount Instrument[int64]
}

// Recorder returns a new Recorder instance.
//
// pkg is the path to the Go package that is performing the instrumentation. If
// it is an internal package, use the package path of the public parent package
// instead.
func (p *Provider) Recorder(pkg string, attrs ...Attr) *Recorder {
	r := &Recorder{
		tracer: p.TracerProvider.Tracer(
			pkg,
			tracerVersion,
			trace.WithInstrumentationAttributes(asAttrKeyValues(attrs)...),
		),
		meter: p.MeterProvider.Meter(
			pkg,
			meterVersion,
			metric.WithInstrumentationAttributes(asAttrKeyValues(attrs)...),
		),
		logger: p.LoggerProvider.Logger(
			pkg,
			logVersion,
			log.WithInstrumentationAttributes(asAttrKeyValues(attrs)...),
		),
	}

	r.errorCount = r.Counter("errors", "{error}", "The number of errors that have occurred.")
	r.operationCount = r.Counter("operations", "{operation}", "The number of operations that have been performed.")
	r.operationsInFlightCount = r.UpDownCounter("operations.in_flight", "{operation}", "The number of operations that are currently in progress.")

	return r
}

var (
	// tracerVersion is a TracerOption that sets the instrumentation version
	// to the current version of the module.
	tracerVersion trace.TracerOption

	// meterVersion is a MeterOption that sets the instrumentation version to
	// the current version of the module.
	meterVersion metric.MeterOption

	// logVersion is a LoggerOption that sets the instrumentation version to
	// the current version of the module.
	logVersion log.LoggerOption
)

func init() {
	const modulePath = "github.com/dogmatiq/persistencekit"
	version := "unknown"

	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == modulePath {
				version = dep.Version
				break
			}
		}
	}

	tracerVersion = trace.WithInstrumentationVersion(version)
	meterVersion = metric.WithInstrumentationVersion(version)
	logVersion = log.WithInstrumentationVersion(version)
}
