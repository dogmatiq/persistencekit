package s3_test

import (
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/driver/aws/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3journal"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3kv"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3set"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
)

func TestNew(t *testing.T) {
	client, _ := s3x.NewTestClient(t)
	bucket := xtesting.UniqueName("new")
	cleanupBucket(t, client, bucket)

	d := s3.New(client, bucket)
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		s3journal.NewBinaryStore(client, bucket),
		s3kv.NewBinaryStore(client, bucket),
		s3set.NewBinaryStore(client, bucket),
	)
}

func TestParseURL(t *testing.T) {
	client, endpoint := s3x.NewTestClient(t)
	bucket := xtesting.UniqueName("url")
	cleanupBucket(t, client, bucket)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	open, err := s3.ParseURL("s3://" + endpoint + "/" + bucket + "?region=us-east-1&insecure")
	if err != nil {
		t.Fatal(err)
	}

	d, err := open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
	})

	drivertest.RunTests(
		t,
		d,
		s3journal.NewBinaryStore(client, bucket),
		s3kv.NewBinaryStore(client, bucket),
		s3set.NewBinaryStore(client, bucket),
	)
}

func cleanupBucket(t testing.TB, client *awss3.Client, bucket string) {
	t.Cleanup(func() {
		ctx := xtesting.ContextForCleanup(t)
		if err := s3x.DeleteBucketIfExists(ctx, client, bucket, nil); err != nil {
			t.Error(err)
		}
	})
}
