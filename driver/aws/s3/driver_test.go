package s3_test

import (
	"net/url"
	"testing"

	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xs3"
	"github.com/dogmatiq/persistencekit/driver/aws/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3journal"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3kv"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3set"
	"github.com/dogmatiq/persistencekit/internal/drivertest"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
)

func TestNew(t *testing.T) {
	client, _ := xs3.NewTestClient(t)
	bucket := xtesting.UniqueName("new")
	xs3.CleanupBucket(t, client, bucket)

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
	client, endpoint := xs3.NewTestClient(t)
	bucket := xtesting.UniqueName("url")
	xs3.CleanupBucket(t, client, bucket)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := s3.ParseURL(t.Context(), "s3://"+endpoint+"/"+bucket+"?region=us-east-1&insecure")
	if err != nil {
		t.Fatal(err)
	}

	d, err := cfg.NewDriver(t.Context())
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

func TestFromURL(t *testing.T) {
	t.Run("it returns a working driver", func(t *testing.T) {
		client, endpoint := xs3.NewTestClient(t)
		bucket := xtesting.UniqueName("fromurl")
		xs3.CleanupBucket(t, client, bucket)

		t.Setenv("AWS_ACCESS_KEY_ID", "test")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

		u := &url.URL{Scheme: "s3", Host: endpoint, Path: "/" + bucket, RawQuery: "region=us-east-1&insecure"}
		cfg, err := s3.FromURL(t.Context(), u)
		if err != nil {
			t.Fatal(err)
		}

		d, err := cfg.NewDriver(t.Context())
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
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct {
			Name string
			URL  *url.URL
		}{
			{"wrong scheme", &url.URL{Scheme: "other", Path: "/bucket"}},
			{"empty bucket", &url.URL{Scheme: "s3"}},
			{"bucket with slash", &url.URL{Scheme: "s3", Path: "/bucket/subpath"}},
			{"insecure without host", &url.URL{Scheme: "s3", Path: "/bucket", RawQuery: "insecure"}},
			{"unknown parameter", &url.URL{Scheme: "s3", Path: "/bucket", RawQuery: "unknown=value"}},
		}
		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				_, err := s3.FromURL(t.Context(), tc.URL)
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		}
	})
}
