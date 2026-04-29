package s3kv_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	. "github.com/dogmatiq/persistencekit/driver/aws/s3/s3kv"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestStore(t *testing.T) {
	client, bucket := setup(t)
	kv.RunTests(
		t,
		NewBinaryStore(client, bucket),
	)
}

func BenchmarkStore(b *testing.B) {
	client, bucket := setup(b)
	kv.RunBenchmarks(
		b,
		NewBinaryStore(client, bucket),
	)
}

func setup(t testing.TB) (*s3.Client, string) {
	client := s3x.NewTestClient(t)
	bucket := xtesting.UniqueName("bucket")

	t.Cleanup(func() {
		if err := s3x.DeleteBucketIfExists(
			xtesting.ContextForCleanup(t),
			client,
			bucket,
			nil,
		); err != nil {
			t.Error(err)
		}
	})

	return client, bucket
}
