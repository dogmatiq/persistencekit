package persistencekit

import (
	"context"
	"fmt"
	"net/url"

	"github.com/dogmatiq/persistencekit/driver/aws/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/s3"
	"github.com/dogmatiq/persistencekit/driver/memory"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver provides access to persistence stores backed by a specific backend.
type Driver interface {
	// JournalStore returns the journal store provided by this driver.
	JournalStore() journal.BinaryStore

	// KVStore returns the key/value store provided by this driver.
	KVStore() kv.BinaryStore

	// SetStore returns the set store provided by this driver.
	SetStore() set.BinaryStore

	// Close closes the driver, releasing any resources.
	Close() error
}

// ParseURL parses a driver URL string and returns a function that opens a
// [Driver] for the backend identified by the URL scheme.
//
// The URL scheme selects the backend driver:
//
// # memory
//
// In-memory stores, suitable for testing. Drivers with the same silo name
// share state for the lifetime of the process.
//
//	memory:///<silo>
//
// # postgres / postgresql
//
// PostgreSQL-backed stores using a connection pool. Pool settings can be
// configured via URL query parameters; see [pgxpool.ParseConfig] for the full
// list.
//
//	postgres://user:password@host:port/database
//	postgresql://user:password@host:port/database?pool_max_conns=10
//
// # dynamodb
//
// DynamoDB-backed stores. The path specifies a table name prefix; each
// primitive uses a separate table ("<prefix>-journal", "<prefix>-kv",
// "<prefix>-set").
//
//	dynamodb:///<table-prefix>
//	dynamodb://<host>:<port>/<table-prefix>?region=us-east-1&insecure
//
// # s3
//
// S3-backed stores. The path specifies the bucket name.
//
//	s3:///<bucket>
//	s3://<endpoint>/<bucket>?region=us-east-1&insecure
//
// The dynamodb and s3 schemes support the following query parameters:
//   - region: AWS region (e.g. "us-east-1"); if omitted, resolved from the environment
//   - role_arn: ARN of an IAM role to assume via STS
//   - insecure: use HTTP instead of HTTPS for a custom endpoint (requires a host)
func ParseURL(u string) (func(context.Context) (Driver, error), error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("cannot parse persistence driver URL: %w", err)
	}
	return FromURL(parsed)
}

// FromURL returns a function that opens a [Driver] for the backend identified
// by the URL scheme. See [ParseURL] for supported URL formats.
func FromURL(u *url.URL) (func(context.Context) (Driver, error), error) {
	switch u.Scheme {
	case "memory":
		return fromURL(memory.FromURL, u)
	case "postgres", "postgresql":
		return fromURL(postgres.FromURL, u)
	case "dynamodb":
		return fromURL(dynamodb.FromURL, u)
	case "s3":
		return fromURL(s3.FromURL, u)
	case "":
		return nil, fmt.Errorf("persistence driver URL has no scheme: %q", u)
	default:
		return nil, fmt.Errorf("unsupported persistence driver scheme %q", u.Scheme)
	}
}

// fromURL adapts a per-driver [FromURL] function to return the [Driver]
// interface.
func fromURL[T Driver](
	fromURL func(*url.URL) (func(context.Context) (T, error), error),
	u *url.URL,
) (func(context.Context) (Driver, error), error) {
	open, err := fromURL(u)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context) (Driver, error) {
		return open(ctx)
	}, nil
}
