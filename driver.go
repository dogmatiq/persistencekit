package persistencekit

import (
	"context"
	"errors"
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

// ErrNotSupported is returned by a [Driver] when it does not support a
// particular persistence primitive.
var ErrNotSupported = errors.New("not supported by this driver")

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
// The URL scheme selects the backend driver. The supported schemes are:
//   - memory: in-memory stores, suitable for testing
//   - postgres or postgresql: PostgreSQL-backed stores
//   - dynamodb: DynamoDB-backed stores
//   - s3: S3-backed stores
func NewDriver(rawURL string) (Driver, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("cannot parse persistence driver URL: %w", err)
	}

	switch u.Scheme {
	case "memory":
		return memory.NewDriver(u)
	case "postgres", "postgresql":
		return postgres.NewDriver(u)
	case "dynamodb":
		return dynamodb.NewDriver(u)
	case "s3":
		return s3.NewDriver(u)
	case "":
		return nil, fmt.Errorf("persistence driver URL has no scheme: %q", rawURL)
	default:
		return nil, fmt.Errorf("unsupported persistence driver scheme %q", u.Scheme)
	}
}
