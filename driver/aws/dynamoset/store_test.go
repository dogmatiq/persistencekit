package dynamoset_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/persistencekit/driver/aws/dynamoset"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/internal/testx"
	"github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	client, table := setup(t)
	set.RunTests(
		t,
		NewBinaryStore(client, table),
	)
}

func BenchmarkStore(b *testing.B) {
	client, table := setup(b)
	set.RunBenchmarks(
		b,
		NewBinaryStore(client, table),
	)
}

func setup(t testing.TB) (*dynamodb.Client, string) {
	client := dynamox.NewTestClient(t)
	table := testx.UniqueName("table")

	t.Cleanup(func() {
		if err := dynamox.DeleteTableIfExists(
			testx.ContextForCleanup(t),
			client,
			table,
			nil,
		); err != nil {
			t.Error(err)
		}
	})

	return client, table
}
