package dynamodb_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/driver/aws/dynamodb"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestJournalStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	client := newClient(t)
	table := "journal"

	if err := CreateJournalStoreTable(ctx, client, table); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := deleteTable(ctx, client, table); err != nil {
			t.Fatal(err)
		}

		cancel()
	})

	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			return &JournalStore{
				Client: client,
				Table:  table,
			}
		},
	)
}
