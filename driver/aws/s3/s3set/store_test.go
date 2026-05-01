package s3set_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xs3"
	. "github.com/dogmatiq/persistencekit/driver/aws/s3/s3set"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	client, bucket := setup(t)
	set.RunTests(
		t,
		NewBinaryStore(client, bucket),
	)
}

func BenchmarkStore(b *testing.B) {
	client, bucket := setup(b)
	set.RunBenchmarks(
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
