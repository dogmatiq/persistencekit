package dynamox

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/internal/testx"
	dynamotc "github.com/testcontainers/testcontainers-go/modules/dynamodb"
)

// NewTestClient returns a new DynamoDB client for use in a test.
func NewTestClient(t testing.TB) *dynamodb.Client {
	container, err := dynamotc.Run(
		t.Context(),
		"amazon/dynamodb-local",
		dynamotc.WithDisableTelemetry(),
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
			opts.BaseEndpoint = aws.String("http://" + endpoint)
		},
	)
}
