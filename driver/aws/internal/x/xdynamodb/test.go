package xdynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/internal/x/xtesting"
	"github.com/testcontainers/testcontainers-go"
	dynamotc "github.com/testcontainers/testcontainers-go/modules/dynamodb"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewTestClient returns a new DynamoDB client for use in a test. It also
// returns the endpoint as a host:port string.
func NewTestClient(t testing.TB) (*dynamodb.Client, string) {
	container, err := dynamotc.Run(
		t.Context(),
		"amazon/dynamodb-local",
		dynamotc.WithDisableTelemetry(),
		testcontainers.WithWaitStrategy(
			wait.
				ForHTTP("/").
				WithPort("8000").
				WithStatusCodeMatcher(func(int) bool {
					// Accept any status, we just want to know when it's up.
					return true
				}),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx := xtesting.ContextForCleanup(t)
		if err := container.Terminate(ctx); err != nil {
			t.Log(err)
		}
	})

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
	), endpoint
}

// CleanupTable registers a t.Cleanup that deletes the given tables at the end
// of the test.
func CleanupTable(t testing.TB, client *dynamodb.Client, tables ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := xtesting.ContextForCleanup(t)
		for _, table := range tables {
			if err := DeleteTableIfExists(ctx, client, table, nil); err != nil {
				t.Error(err)
			}
		}
	})
}
