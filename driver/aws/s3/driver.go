package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xaws"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3journal"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3kv"
	"github.com/dogmatiq/persistencekit/driver/aws/s3/s3set"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is a persistence driver backed by Amazon S3.
type Driver struct {
	bucket string
	client *awss3.Client
}

// New returns a [Driver] that uses the given S3 client and bucket.
func New(client *awss3.Client, bucket string) *Driver {
	return &Driver{
		client: client,
		bucket: bucket,
	}
}

// Config holds the configuration for an S3 persistence driver.
type Config struct {
	// AWS is the AWS configuration used to create the S3 client.
	AWS aws.Config

	// ClientOptions are additional options applied to the S3 client.
	ClientOptions []func(*awss3.Options)

	// Bucket is the S3 bucket name.
	Bucket string
}

// NewDriver creates a [Driver] using the configured AWS settings.
func (c *Config) NewDriver(context.Context) (*Driver, error) {
	return &Driver{
		bucket: c.Bucket,
		client: awss3.NewFromConfig(c.AWS, c.ClientOptions...),
	}, nil
}

// ParseURL returns a [*Config] for the given s3:// URL string.
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
func ParseURL(ctx context.Context, u string) (*Config, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid s3 URL: %w", err)
	}
	return FromURL(ctx, parsed)
}

// FromURL returns a [*Config] for the given s3:// [*url.URL]. See [ParseURL]
// for the URL format.
func FromURL(ctx context.Context, u *url.URL) (*Config, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid s3 URL: unexpected scheme %q", u.Scheme)
	}

	bucket := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return nil, errors.New("invalid s3 URL: bucket name is required in the path (e.g. s3:///<bucket>)")
	}
	if strings.Contains(bucket, "/") {
		return nil, errors.New("invalid s3 URL: bucket name must not contain '/' (e.g. s3:///<bucket>)")
	}

	usePathStyle := u.Host != ""

	cfg, err := xaws.LoadConfig(ctx, u)
	if err != nil {
		return nil, err
	}

	var clientOpts []func(*awss3.Options)
	if usePathStyle {
		clientOpts = append(
			clientOpts,
			func(opts *awss3.Options) {
				opts.UsePathStyle = true
			},
		)
	}

	return &Config{
		AWS:           cfg,
		ClientOptions: clientOpts,
		Bucket:        bucket,
	}, nil
}

// JournalStore returns a journal store backed by S3.
func (d *Driver) JournalStore() journal.BinaryStore {
	return s3journal.NewBinaryStore(d.client, d.bucket)
}

// KVStore returns a key/value store backed by S3.
func (d *Driver) KVStore() kv.BinaryStore {
	return s3kv.NewBinaryStore(d.client, d.bucket)
}

// SetStore returns a set store backed by S3.
func (d *Driver) SetStore() set.BinaryStore {
	return s3set.NewBinaryStore(d.client, d.bucket)
}

// Close is a no-op. The S3 client does not require explicit cleanup.
func (d *Driver) Close() error {
	return nil
}
