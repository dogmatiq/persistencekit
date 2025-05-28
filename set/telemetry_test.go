package set_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	. "github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/spruce"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestWithTelemetry(t *testing.T) {
	RunTests(
		t,
		WithTelemetry(
			&memoryset.BinaryStore{},
			nooptrace.NewTracerProvider(),
			noopmetric.NewMeterProvider(),
			spruce.NewTestLogger(t),
		),
	)
}
