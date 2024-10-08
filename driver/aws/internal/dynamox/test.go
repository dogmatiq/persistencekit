package dynamox

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewTestClient returns a new DynamoDB client for use in a test.
func NewTestClient(t testing.TB) *dynamodb.Client {
	endpoint := os.Getenv("DOGMATIQ_TEST_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:28000"
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("id", "secret", ""),
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

	return dynamodb.NewFromConfig(
		cfg,
		func(opts *dynamodb.Options) {
			opts.BaseEndpoint = aws.String(endpoint)
		},
	)
}
