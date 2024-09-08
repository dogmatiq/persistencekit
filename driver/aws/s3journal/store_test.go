package s3journal_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	. "github.com/dogmatiq/persistencekit/driver/aws/s3journal"
	"github.com/dogmatiq/persistencekit/internal/testx"
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
	client := s3x.NewTestClient(t)
	bucket := testx.UniqueName("bucket")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := s3x.DeleteBucketIfExists(ctx, client, bucket, nil); err != nil {
			t.Error(err)
		}
	})

	return client, bucket
}
