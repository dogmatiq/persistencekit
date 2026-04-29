package s3x

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

// NewTestClient returns a new S3 client for use in a test. It also returns the
// endpoint as a host:port string.
func NewTestClient(t testing.TB) (*s3.Client, string) {
	container, err := localstack.Run(
		t.Context(),
		"localstack/localstack:4",
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx := xtesting.ContextForCleanup(t)
		if err := container.Terminate(ctx); err != nil {
			t.Log(err)
		}
	})

	mappedPort, err := container.MappedPort(t.Context(), "4566/tcp")
	if err != nil {
		t.Fatal(err)
	}

	host, err := container.Host(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	hostPort := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	endpoint := fmt.Sprintf("http://%s", hostPort)

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			),
		),
		config.WithRetryer(
			func() aws.Retryer {
				return aws.NopRetryer{}
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s3.NewFromConfig(
		cfg,
		func(opts *s3.Options) {
			opts.BaseEndpoint = aws.String(endpoint)
			opts.UsePathStyle = true
		},
	), hostPort
}
