package set_test

import (
	"testing"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	. "github.com/dogmatiq/persistencekit/set"
	nooplog "go.opentelemetry.io/otel/log/noop"
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
			nooplog.NewLoggerProvider(),
		),
	)
}
