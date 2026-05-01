package memory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/dogmatiq/persistencekit/driver"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

var silos sync.Map

type silo struct {
	kv      memorykv.BinaryStore
	journal memoryjournal.BinaryStore
	set     memoryset.BinaryStore
}

// Driver is a persistence driver backed by a named in-memory silo.
type Driver struct {
	silo *silo
}

// New returns a [Driver] described by the given configuration.
func New(cfg *Config) *Driver {
	v, _ := silos.LoadOrStore(cfg.Silo, &silo{})
	return &Driver{silo: v.(*silo)}
}

// Config holds the configuration for an in-memory persistence driver.
type Config struct {
	// Silo is the name of the shared in-memory silo. Drivers with the same
	// silo name share state for the lifetime of the process.
	Silo string
}

// NewDriver returns a [Driver] described by the given configuration.
func (c *Config) NewDriver(context.Context) (driver.Driver, error) {
	return New(c), nil
}

// ParseURL returns a [Config] for the given URL string.
//
// URL format:
//
//	memory:///<silo>
//
// The silo name identifies a shared in-memory store. Drivers with the same silo
// name share state for the lifetime of the process.
func ParseURL(ctx context.Context, u string) (*Config, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid memory URL: %w", err)
	}
	return FromURL(ctx, parsed)
}

// FromURL returns a [Config] for the given URL.
//
// See [ParseURL] for the URL format.
func FromURL(_ context.Context, u *url.URL) (*Config, error) {
	if u.Scheme != "memory" {
		return nil, fmt.Errorf("invalid memory URL: unexpected scheme %q", u.Scheme)
	}

	if u.Host != "" {
		return nil, fmt.Errorf("invalid memory URL: host component must be empty, use memory:///<silo> for a named silo")
	}

	if u.RawQuery != "" {
		return nil, fmt.Errorf("invalid memory URL: query parameters are not supported")
	}

	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return nil, fmt.Errorf("invalid memory URL: silo name is required in the URL path: memory:///<silo>")
	}

	return &Config{Silo: name}, nil
}

// JournalStore returns the silo's in-memory journal store.
func (d *Driver) JournalStore() journal.BinaryStore {
	return &d.silo.journal
}

// KVStore returns the silo's in-memory key/value store.
func (d *Driver) KVStore() kv.BinaryStore {
	return &d.silo.kv
}

// SetStore returns the silo's in-memory set store.
func (d *Driver) SetStore() set.BinaryStore {
	return &d.silo.set
}

// Close is a no-op. The silo's state persists for the lifetime of the process.
func (d *Driver) Close() error {
	return nil
}
