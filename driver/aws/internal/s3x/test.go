package s3x

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/internal/testx"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// NewTestClient returns a new S3 client for use in a test.
func NewTestClient(t testing.TB) *s3.Client {
	username := "persistencekit"
	password := uuid.NewString()

	container, err := minio.Run(
		t.Context(),
		"minio/minio",
		minio.WithUsername(username),
		minio.WithPassword(password),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx := testx.ContextForCleanup(t)
		if err := container.Terminate(ctx); err != nil {
			t.Log(err)
		}
	})

	endpoint, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				username,
				password,
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
			opts.BaseEndpoint = aws.String("http://" + endpoint)
			opts.UsePathStyle = true
		},
	)
}
