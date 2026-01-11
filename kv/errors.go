package kv

import (
	"encoding/hex"
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

// ConflictError is returned by [Journal.Append] if there is already a record at
// the specified position.
type ConflictError[K any] struct {
	Keyspace string
	Key      K
	Token    []byte
}

func (e ConflictError[K]) Error() string {
	token := "<empty>"
	if len(e.Token) > 0 {
		token = hex.EncodeToString(e.Token)
	}

	return fmt.Sprintf("the supplied concurrency token (%s) for key %v in the %q keyspace does not match the expected token", token, e.Key, e.Keyspace)
}

// isConflictError provides a common interface to detect [ConflictError] using
// [errors.As] without knowing the type of K.
func (ConflictError[K]) isConflictError() {}
