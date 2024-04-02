package journal

import "errors"

var (
	// ErrNotFound is returned by [Journal.Get] and [Journal.Range] if the
	// requested record does not exist, either because it has been truncated or
	// because the given position has not been written yet.
	ErrNotFound = errors.New("record not found")

	// ErrConflict is returned by [Journal.Append] if there is already a record at
	// the specified position.
	ErrConflict = errors.New("optimistic concurrency conflict")
)

// IgnoreNotFound returns nil if err is [ErrNotFound], otherwise it returns err.
func IgnoreNotFound(err error) error {
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}
