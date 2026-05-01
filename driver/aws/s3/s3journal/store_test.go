package s3journal_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xs3"
	. "github.com/dogmatiq/persistencekit/driver/aws/s3/s3journal"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/dogmatiq/persistencekit/journal"
)

func TestStore(t *testing.T) {
	client, bucket := setup(t)
	journal.RunTests(
		t,
		NewBinaryStore(client, bucket),
	)
}

func BenchmarkStore(b *testing.B) {
	client, bucket := setup(b)
	journal.RunBenchmarks(
		b,
		NewBinaryStore(client, bucket),
	)
}

func setup(t testing.TB) (*s3.Client, string) {
	client, _ := xs3.NewTestClient(t)
	bucket := xtesting.UniqueName("bucket")
	xs3.CleanupBucket(t, client, bucket)
	return client, bucket
}
