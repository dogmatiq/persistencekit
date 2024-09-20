package journal

import (
	"errors"
	"fmt"
)

var (
	// ErrConflict is returned by [Journal.Append] if there is already a record at
	// the specified position.
	ErrConflict = errors.New("optimistic concurrency conflict")
)

// IgnoreNotFound returns nil if err is a caused by [RecordNotFoundError] or
// [ValueNotFoundError] error. Otherwise it returns err unchanged.
func IgnoreNotFound(err error) error {
	if IsNotFound(err) {
		return nil
	}
	return err
}

// IsNotFound returns true if err is is caused by [RecordNotFoundError] or
// [ValueNotFoundError].
func IsNotFound(err error) bool {
	if errors.As(err, &RecordNotFoundError{}) {
		return true
	}

	if errors.As(err, &ValueNotFoundError{}) {
		return true
	}

	return false
}

// RecordNotFoundError is returned by [Journal.Get] and [Journal.Range] if the
// requested record does not exist, either because it has been truncated or
// because the given position has not been written yet.
type RecordNotFoundError struct {
	Position Position
}

func (e RecordNotFoundError) Error() string {
	return fmt.Sprintf("record at position %d has not been appended yet, or has been truncated", e.Position)
}

// ValueNotFoundError is returned search and can operations if the target
// value is not found.
type ValueNotFoundError struct{}

func (e ValueNotFoundError) Error() string {
	return "target value not found"
}
