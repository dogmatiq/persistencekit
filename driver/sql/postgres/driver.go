package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/dogmatiq/persistencekit/driver"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Driver is a persistence driver backed by PostgreSQL.
type Driver struct {
	pool *pgxpool.Pool
	db   *sql.DB
}

// New returns a [Driver] described by the given configuration.
func New(cfg *Config) (*Driver, error) {
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg.Pool)
	if err != nil {
		return nil, err
	}

	return &Driver{
		pool: pool,
		db:   stdlib.OpenDBFromPool(pool),
	}, nil
}

// NewFromDB returns a [Driver] that uses the given [*sql.DB]. The caller
// retains ownership of db; [Driver.Close] does not close it.
func NewFromDB(db *sql.DB) *Driver {
	return &Driver{db: db}
}

// Config holds the configuration for a PostgreSQL persistence driver.
type Config struct {
	// Pool is the pgxpool configuration used to establish connections.
	Pool *pgxpool.Config
}

// NewDriver returns a [Driver] described by the given configuration.
func (c *Config) NewDriver(context.Context) (driver.Driver, error) {
	return New(c)
}

// ParseURL returns a [Config] for the given URL string.
//
// URL format:
//
//	postgres://[user:password@]host[:port]/database[?parameters]
//
// Pool and connection settings can be configured via URL parameters. See
// [pgxpool.ParseConfig] for the full list of supported parameters.
func ParseURL(ctx context.Context, u string) (*Config, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}
	return FromURL(ctx, parsed)
}

// FromURL returns a [Config] for the given URL.
//
// See [ParseURL] for the URL format.
func FromURL(_ context.Context, u *url.URL) (*Config, error) {
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("invalid postgres URL: unexpected scheme %q", u.Scheme)
	}

	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}

	return &Config{Pool: cfg}, nil
}

// JournalStore returns a journal store backed by PostgreSQL.
func (d *Driver) JournalStore() journal.BinaryStore {
	return &pgjournal.BinaryStore{DB: d.db}
}

// KVStore returns a key/value store backed by PostgreSQL.
func (d *Driver) KVStore() kv.BinaryStore {
	return &pgkv.BinaryStore{DB: d.db}
}

// SetStore returns a set store backed by PostgreSQL.
func (d *Driver) SetStore() set.BinaryStore {
	return &pgset.BinaryStore{DB: d.db}
}

// Close closes the underlying connection pool.
func (d *Driver) Close() error {
	if d.pool == nil {
		return nil
	}

	err := d.db.Close()
	d.pool.Close()
	return err
}
