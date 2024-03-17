package journal_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/spruce"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestWithTelemetry(t *testing.T) {
	RunTests(
		t,
		func(t *testing.T) BinaryStore {
			return WithTelemetry(
				&memoryjournal.BinaryStore{},
				nooptrace.NewTracerProvider(),
				noopmetric.NewMeterProvider(),
				spruce.NewLogger(t),
			)
		},
	)
}
