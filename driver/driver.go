// Package driver defines the interfaces implemented by persistence drivers.
package driver

import (
	"context"

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
