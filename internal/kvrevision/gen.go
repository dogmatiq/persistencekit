package kvrevision

import (
	"strconv"

	"github.com/dogmatiq/persistencekit/kv"
)

// MarshalGeneration converts a generation counter to its [kv.Revision] representation.
func MarshalGeneration(gen uint64) kv.Revision {
	return kv.Revision(strconv.FormatUint(gen, 10))
}

// UnmarshalGeneration returns the generation counter encoded in r.
// It panics if r is not a valid generation encoding.
func UnmarshalGeneration(r kv.Revision) uint64 {
	gen, ok := TryUnmarshalGeneration(r)
	if !ok {
		panic("invalid generation encoding: " + string(r))
	}
	return gen
}

// TryUnmarshalGeneration returns the generation counter encoded in r, and
// whether r is a valid generation encoding. An empty revision is treated as
// generation 0.
func TryUnmarshalGeneration(r kv.Revision) (uint64, bool) {
	if r == "" {
		return 0, true
	}
	gen, err := strconv.ParseUint(string(r), 10, 64)
	if err != nil || gen == 0 {
		return 0, false
	}
	return gen, string(r) == strconv.FormatUint(gen, 10)
}

// IncrementGeneration returns the next revision after r, assuming r encodes a
// generation counter.
func IncrementGeneration(r kv.Revision) kv.Revision {
	if r == "" {
		return MarshalGeneration(1)
	}
	return MarshalGeneration(UnmarshalGeneration(r) + 1)
}
