package clone

import (
	"github.com/dogmatiq/dyad"
	"google.golang.org/protobuf/proto"
)

// Clone returns a deep copy of v.
func Clone[T any](v T) T {
	if v, ok := any(v).(proto.Message); ok {
		return proto.Clone(v).(T)
	}
	return dyad.Clone(v)
}
