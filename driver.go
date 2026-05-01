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

// Driver provides access to the persistence stores of a specific driver.
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

// Config describes the connection parameters for a persistence driver.
type Config interface {
	// NewDriver creates a new [Driver] using this configuration.
	NewDriver(context.Context) (Driver, error)
}

// ParseURL parses a driver URL string and returns a [Config] for the backend
// identified by the URL scheme.
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
func ParseURL(ctx context.Context, u string) (Config, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("cannot parse persistence driver URL: %w", err)
	}
	return FromURL(ctx, parsed)
}

// FromURL returns a [Config] for the backend identified by the URL scheme. See
// [ParseURL] for supported URL formats.
func FromURL(ctx context.Context, u *url.URL) (Config, error) {
	switch u.Scheme {
	case "memory":
		return asInterface(memory.FromURL(ctx, u))
	case "postgres", "postgresql":
		return asInterface(postgres.FromURL(ctx, u))
	case "dynamodb":
		return asInterface(dynamodb.FromURL(ctx, u))
	case "s3":
		return asInterface(s3.FromURL(ctx, u))
	case "":
		return nil, fmt.Errorf("persistence driver URL has no scheme: %q", u)
	default:
		return nil, fmt.Errorf("unsupported persistence driver scheme %q", u.Scheme)
	}
}

// config is the constraint satisfied by per-driver *Config types whose
// NewDriver method returns a concrete *Driver rather than the [Driver]
// interface.
type config[T Driver] interface {
	NewDriver(context.Context) (T, error)
}

// asInterface adapts a driver-specific [config] to the generic [Config]
// interface.
func asInterface[T Driver](cfg config[T], err error) (Config, error) {
	if err != nil {
		return nil, err
	}

	return &configAdapter[T]{cfg}, nil
}

type configAdapter[T Driver] struct {
	cfg config[T]
}

func (a *configAdapter[T]) NewDriver(ctx context.Context) (Driver, error) {
	return a.cfg.NewDriver(ctx)
}
