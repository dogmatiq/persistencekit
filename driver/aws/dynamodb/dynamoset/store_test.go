package dynamoset_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	. "github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamoset"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xdynamodb"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
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
	client, _ := xdynamodb.NewTestClient(t)
	table := xtesting.UniqueName("table")
	xdynamodb.CleanupTable(t, client, table)
	return client, table
}
