package dynamojournal_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/persistencekit/driver/aws/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestJournalStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	client := dynamox.NewTestClient(t)
	table := "journal"

	if err := CreateTable(ctx, client, table); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := dynamox.DeleteTableIfNotExists(ctx, client, table); err != nil {
			t.Fatal(err)
		}

		cancel()
	})

	journal.RunTests(
		t,
		func(t *testing.T) journal.Store {
			return &Store{
				Client: client,
				Table:  table,
			}
		},
	)
}
