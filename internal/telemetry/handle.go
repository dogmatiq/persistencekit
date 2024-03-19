package telemetry

import (
	"fmt"
	"sync/atomic"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

var handleCounter atomic.Uint64

// HandleID returns a unique identifier for an open instance of a journal or
// keyspace.
//
// It includes a counter component for easy visual identification by humans, and
// a UUID component for global correlation in observability tools.
func HandleID() string {
	return fmt.Sprintf(
		"#%d %s",
		handleCounter.Add(1),
		uuidpb.Generate().AsString(),
	)
}
