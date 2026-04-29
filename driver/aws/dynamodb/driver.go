package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb/dynamoset"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is a persistence driver backed by Amazon DynamoDB.
type Driver struct {
	tablePrefix string
	client      *awsdynamodb.Client
}

// New returns a [Driver] that uses the given DynamoDB client and table prefix.
func New(client *awsdynamodb.Client, tablePrefix string) *Driver {
	return &Driver{
		client:      client,
		tablePrefix: tablePrefix,
	}
}

// ParseURL returns a function that opens a [Driver] configured by the given
// dynamodb:// URL string.
//
// URL format:
//
//	dynamodb:///<table-prefix>
//	dynamodb://<host>:<port>/<table-prefix>
//
// Supported query parameters:
//   - region: AWS region (e.g. "us-east-1"); if omitted, resolved from the environment
//   - role_arn: ARN of an IAM role to assume via STS
//   - insecure: use HTTP instead of HTTPS for a custom endpoint (requires a host)
func ParseURL(u string) (func(context.Context) (*Driver, error), error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid dynamodb URL: %w", err)
	}
	return FromURL(parsed)
}

// FromURL returns a function that opens a [Driver] configured by the given
// dynamodb:// [*url.URL]. See [ParseURL] for the URL format.
func FromURL(u *url.URL) (func(context.Context) (*Driver, error), error) {
	if u.Scheme != "dynamodb" {
		return nil, fmt.Errorf("invalid dynamodb URL: unexpected scheme %q", u.Scheme)
	}

	tablePrefix := strings.TrimPrefix(u.Path, "/")
	if tablePrefix == "" {
		return nil, errors.New("invalid dynamodb URL: table prefix is required in the path (e.g. dynamodb:///<table-prefix>)")
	}

	loadConfig, err := awsx.ParseConfig(u)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) (*Driver, error) {
		cfg, err := loadConfig(ctx)
		if err != nil {
			return nil, err
		}

		return &Driver{
			tablePrefix: tablePrefix,
			client:      awsdynamodb.NewFromConfig(cfg),
		}, nil
	}, nil
}

// JournalStore returns a journal store backed by DynamoDB.
func (d *Driver) JournalStore() journal.BinaryStore {
	return dynamojournal.NewBinaryStore(d.client, d.tablePrefix+"-journal")
}

// KVStore returns a key/value store backed by DynamoDB.
func (d *Driver) KVStore() kv.BinaryStore {
	return dynamokv.NewBinaryStore(d.client, d.tablePrefix+"-kv")
}

// SetStore returns a set store backed by DynamoDB.
func (d *Driver) SetStore() set.BinaryStore {
	return dynamoset.NewBinaryStore(d.client, d.tablePrefix+"-set")
}

// Close is a no-op. The DynamoDB client does not require explicit cleanup.
func (d *Driver) Close() error {
	return nil
}
