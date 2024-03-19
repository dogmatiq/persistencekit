package dynamojournal_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/persistencekit/driver/aws/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/spruce"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestStore(t *testing.T) {
	client, table := setup(t)
	journal.RunTests(
		t,
		journal.WithTelemetry( // TODO: remove telemetry
			NewBinaryStore(client, table),
			nooptrace.NewTracerProvider(),
			noopmetric.NewMeterProvider(),
			spruce.NewLogger(t),
		),
	)
}

func BenchmarkStore(b *testing.B) {
	client, table := setup(b)
	journal.RunBenchmarks(
		b,
		NewBinaryStore(client, table),
	)
}

func setup(t testing.TB) (*dynamodb.Client, string) {
	client := dynamox.NewTestClient(t)
	table := "journal"

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := dynamox.DeleteTableIfNotExists(ctx, client, table); err != nil {
			t.Error(err)
		}
	})

	return client, table
}
