package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// Config holds the configuration for a DynamoDB persistence driver.
type Config struct {
	// AWS is the AWS configuration used to create the DynamoDB client.
	AWS aws.Config

	// ClientOptions are additional options applied to the DynamoDB client.
	ClientOptions []func(*awsdynamodb.Options)

	// TablePrefix is the prefix for DynamoDB table names. Each primitive uses a
	// separate table ("<prefix>-journal", "<prefix>-kv", "<prefix>-set").
	TablePrefix string
}

// NewDriver creates a [Driver] using the configured AWS settings.
func (c *Config) NewDriver(context.Context) (*Driver, error) {
	return &Driver{
		tablePrefix: c.TablePrefix,
		client:      awsdynamodb.NewFromConfig(c.AWS, c.ClientOptions...),
	}, nil
}

// ParseURL returns a [*Config] for the given dynamodb:// URL string.
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
func ParseURL(ctx context.Context, u string) (*Config, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid dynamodb URL: %w", err)
	}
	return FromURL(ctx, parsed)
}

// FromURL returns a [*Config] for the given dynamodb:// [*url.URL]. See
// [ParseURL] for the URL format.
func FromURL(ctx context.Context, u *url.URL) (*Config, error) {
	if u.Scheme != "dynamodb" {
		return nil, fmt.Errorf("invalid dynamodb URL: unexpected scheme %q", u.Scheme)
	}

	tablePrefix := strings.TrimPrefix(u.Path, "/")
	if tablePrefix == "" {
		return nil, errors.New("invalid dynamodb URL: table prefix is required in the path (e.g. dynamodb:///<table-prefix>)")
	}

	cfg, err := awsx.LoadConfig(ctx, u)
	if err != nil {
		return nil, err
	}

	return &Config{
		AWS:         cfg,
		TablePrefix: tablePrefix,
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
