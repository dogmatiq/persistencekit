package dynamojournal_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/persistencekit/driver/aws/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	client, table := setup(t)
	journal.RunTests(
		t,
		func(t *testing.T) journal.BinaryStore {
			return NewBinaryStore(client, table)
		},
	)
}

func BenchmarkStore(b *testing.B) {
	client, table := setup(b)
	journal.RunBenchmarks(
		b,
		func(b *testing.B) journal.BinaryStore {
			return NewBinaryStore(client, table)
		},
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
