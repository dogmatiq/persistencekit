package dynamokv_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/persistencekit/driver/aws/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestStore(t *testing.T) {
	client, table := setup(t)
	kv.RunTests(
		t,
		NewBinaryStore(client, table),
	)
}

func BenchmarkStore(b *testing.B) {
	client, table := setup(b)
	kv.RunBenchmarks(
		b,
		NewBinaryStore(client, table),
	)
}

func setup(t testing.TB) (*dynamodb.Client, string) {
	client := dynamox.NewTestClient(t)
	table := "kvstore"

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := dynamox.DeleteTableIfNotExists(ctx, client, table, nil); err != nil {
			t.Error(err)
		}
	})

	return client, table
}
