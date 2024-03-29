package telemetry

import (
	"runtime/debug"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	// tracerVersion is a TracerOption that sets the instrumentation version
	// to the current version of the Veracity module.
	tracerVersion trace.TracerOption

	// meterVersion is a MeterOption that sets the instrumentation version to
	// the current version of the Veracity module.
	meterVersion metric.MeterOption
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
}
