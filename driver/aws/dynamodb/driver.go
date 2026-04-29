package dynamodb

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"

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
	loadConfig  awsx.ConfigLoader

	m      sync.Mutex
	client *awsdynamodb.Client
}

// NewDriver returns a [Driver] configured from a dynamodb:// URL.
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
func NewDriver(u *url.URL) (*Driver, error) {
	tablePrefix := strings.TrimPrefix(u.Path, "/")
	if tablePrefix == "" {
		return nil, errors.New("invalid dynamodb URL: table prefix is required in the path (e.g. dynamodb:///<table-prefix>)")
	}

	loadConfig, err := awsx.ParseConfig(u)
	if err != nil {
		return nil, err
	}

	return &Driver{
		tablePrefix: tablePrefix,
		loadConfig:  loadConfig,
	}, nil
}

// JournalStore returns a journal store backed by DynamoDB.
func (d *Driver) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamojournal.NewBinaryStore(client, d.tablePrefix+"-journal"), nil
}

// KVStore returns a key/value store backed by DynamoDB.
func (d *Driver) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamokv.NewBinaryStore(client, d.tablePrefix+"-kv"), nil
}

// SetStore returns a set store backed by DynamoDB.
func (d *Driver) SetStore(ctx context.Context) (set.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamoset.NewBinaryStore(client, d.tablePrefix+"-set"), nil
}

// Close is a no-op. The DynamoDB client does not require explicit cleanup.
func (d *Driver) Close() error {
	return nil
}

func (d *Driver) open(ctx context.Context) (*awsdynamodb.Client, error) {
	d.m.Lock()
	defer d.m.Unlock()

	if d.client != nil {
		return d.client, nil
	}

	cfg, err := d.loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	d.client = awsdynamodb.NewFromConfig(cfg)
	return d.client, nil
}
