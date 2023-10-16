package dynamokv_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/driver/aws/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	client := dynamox.NewTestClient(t)
	table := "kvstore"

	if err := CreateTable(ctx, client, table); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := dynamox.DeleteTableIfNotExists(ctx, client, table); err != nil {
			t.Fatal(err)
		}

		cancel()
	})

	kv.RunTests(
		t,
		func(t *testing.T) kv.Store {
			return &Store{
				Client: client,
				Table:  table,
			}
		},
	)
}
