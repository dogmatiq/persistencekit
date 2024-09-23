package s3x

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// NewTestClient returns a new S3 client for use in a test.
func NewTestClient(t testing.TB) *s3.Client {
	endpoint := os.Getenv("DOGMATIQ_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:29000"
	}

	accessKey := os.Getenv("DOGMATIQ_TEST_MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minio"
	}

	secretKey := os.Getenv("DOGMATIQ_TEST_MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "password"
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
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
	)
}
