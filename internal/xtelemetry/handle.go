package xtelemetry

import (
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
)

var handleCounter atomic.Uint64

// HandleID returns a unique identifier for an open instance of a persistence
// primitive, such as a journal.
//
// It includes a counter component for easy visual identification by humans, and
// a UUID component for global correlation in observability tools.
func HandleID() string {
	return fmt.Sprintf(
		"#%d %s",
		handleCounter.Add(1),
		uuid.NewString(),
	)
}
