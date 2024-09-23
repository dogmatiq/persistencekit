package errorx

import (
	"fmt"

	"github.com/dogmatiq/persistencekit/journal"
)

// Wrap adds additional context to an error.
func Wrap(err *error, format string, args ...any) {
	if err == nil {
		panic("err must not be nil")
	}

	if *err == nil {
		return
	}

	if journal.IsNotFound(*err) {
		return
	}

	if journal.IsConflict(*err) {
		return
	}

	*err = fmt.Errorf(format+": %w", append(args, *err)...)
}
