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
	JournalStore(ctx context.Context) (journal.BinaryStore, error)

	// KVStore returns the key/value store provided by this driver.
	KVStore(ctx context.Context) (kv.BinaryStore, error)

	// SetStore returns the set store provided by this driver.
	SetStore(ctx context.Context) (set.BinaryStore, error)

	// Close closes the driver, releasing any resources.
	Close() error
}

// NewDriver returns a new [Driver] for the backend identified by the given URL.
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
func NewDriver(u string) (Driver, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("cannot parse persistence driver URL: %w", err)
	}

	switch parsed.Scheme {
	case "memory":
		return memory.NewDriver(parsed)
	case "postgres", "postgresql":
		return postgres.NewDriver(parsed)
	case "dynamodb":
		return dynamodb.NewDriver(parsed)
	case "s3":
		return s3.NewDriver(parsed)
	case "":
		return nil, fmt.Errorf("persistence driver URL has no scheme: %q", u)
	default:
		return nil, fmt.Errorf("unsupported persistence driver scheme %q", parsed.Scheme)
	}
}
