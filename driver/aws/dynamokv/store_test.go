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
		func(t *testing.T) kv.BinaryStore {
			return &BinaryStore{
				Client: client,
				Table:  table,
			}
		},
	)
}

func BenchmarkStore(b *testing.B) {
	client, table := setup(b)
	kv.RunBenchmarks(
		b,
		func(b *testing.B) kv.BinaryStore {
			return &BinaryStore{
				Client: client,
				Table:  table,
			}
		},
	)
}

func setup(t testing.TB) (*dynamodb.Client, string) {
	client := dynamox.NewTestClient(t)
	table := "kvstore"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := CreateTable(ctx, client, table); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := dynamox.DeleteTableIfNotExists(ctx, client, table); err != nil {
			t.Fatal(err)
		}
	})

	return client, table
}
