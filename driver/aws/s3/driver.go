package s3

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3journal"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3kv"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3set"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is a persistence driver backed by Amazon S3.
type Driver struct {
	bucket     string
	loadConfig awsx.ConfigLoader

	m      sync.Mutex
	client *awss3.Client
}

// NewDriver returns a [Driver] configured from an s3:// URL.
//
// URL format:
//
//	s3:///<bucket>
//	s3://<endpoint>/<bucket>
//
// Supported query parameters:
//   - region: AWS region (e.g. "us-east-1"); if omitted, resolved from the environment
//   - role_arn: ARN of an IAM role to assume via STS
//   - insecure: use HTTP instead of HTTPS for a custom endpoint (requires a host)
func NewDriver(u *url.URL) (*Driver, error) {
	bucket := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return nil, errors.New("invalid s3 URL: bucket name is required in the path (e.g. s3:///<bucket>)")
	}
	if strings.Contains(bucket, "/") {
		return nil, errors.New("invalid s3 URL: bucket name must not contain '/' (e.g. s3:///<bucket>)")
	}

	loadConfig, err := awsx.ParseConfig(u)
	if err != nil {
		return nil, err
	}

	return &Driver{
		bucket:     bucket,
		loadConfig: loadConfig,
	}, nil
}

// JournalStore returns a journal store backed by S3.
func (d *Driver) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3journal.NewBinaryStore(client, d.bucket), nil
}

// KVStore returns a key/value store backed by S3.
func (d *Driver) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3kv.NewBinaryStore(client, d.bucket), nil
}

// SetStore returns a set store backed by S3.
func (d *Driver) SetStore(ctx context.Context) (set.BinaryStore, error) {
	client, err := d.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3set.NewBinaryStore(client, d.bucket), nil
}

// Close is a no-op. The S3 client does not require explicit cleanup.
func (d *Driver) Close() error {
	return nil
}

func (d *Driver) open(ctx context.Context) (*awss3.Client, error) {
	d.m.Lock()
	defer d.m.Unlock()

	if d.client != nil {
		return d.client, nil
	}

	cfg, err := d.loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	d.client = awss3.NewFromConfig(cfg)
	return d.client, nil
}
