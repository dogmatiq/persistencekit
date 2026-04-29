package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

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

// New returns a [Driver] that uses the given [*sql.DB]. The caller retains
// ownership of db; [Driver.Close] does not close it.
func New(db *sql.DB) *Driver {
	return &Driver{db: db}
}

// ParseURL returns a function that opens a [Driver] configured by the given
// postgres:// or postgresql:// URL string.
//
// URL format:
//
//	postgres://[user:password@]host[:port]/database[?parameters]
//
// Pool and connection settings can be configured via URL parameters. See
// [pgxpool.ParseConfig] for the full list of supported parameters.
func ParseURL(u string) (func(context.Context) (*Driver, error), error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}
	return FromURL(parsed)
}

// FromURL returns a function that opens a [Driver] configured by the given
// postgres:// or postgresql:// [*url.URL]. See [ParseURL] for the URL format.
func FromURL(u *url.URL) (func(context.Context) (*Driver, error), error) {
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("invalid postgres URL: unexpected scheme %q", u.Scheme)
	}

	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}

	return func(ctx context.Context) (*Driver, error) {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}

		return &Driver{
			pool: pool,
			db:   stdlib.OpenDBFromPool(pool),
		}, nil
	}, nil
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
