package kv

import (
	"errors"
	"fmt"
)

// IsConflict returns true if err is caused by [ConflictError].
func IsConflict(err error) bool {
	var target interface {
		isConflictError()
	}

	return errors.As(err, &target)
}

// ConflictError is returned by [Keyspace.Set] if the supplied revision does not
// match the key's actual revision.
type ConflictError[K any] struct {
	// Keyspace is the name of the keyspace in which the conflict occurred.
	Keyspace string

	// Key is the key on which the conflict occurred.
	Key K

	// Revision is the (incorrect) revision supplied to [Keyspace.Set].
	Revision Revision
}

func (e ConflictError[K]) Error() string {
	return fmt.Sprintf(
		"the supplied revision (%d) for key %v in the %q keyspace does not match the current revision",
		e.Revision,
		e.Key,
		e.Keyspace,
	)
}

func (ConflictError[K]) isConflictError() {}
