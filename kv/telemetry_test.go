package kv_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/spruce"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestWithTelemetry(t *testing.T) {
	RunTests(
		t,
		WithTelemetry(
			&memorykv.BinaryStore{},
			nooptrace.NewTracerProvider(),
			noopmetric.NewMeterProvider(),
			spruce.NewLogger(t),
		),
	)
}
