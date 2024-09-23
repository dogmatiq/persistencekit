package testx

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var counters sync.Map

// UniqueName returns a unique name with the given prefix.
func UniqueName(prefix string) string {
	v, ok := counters.Load(prefix)
	if !ok {
		var counter atomic.Uint64
		v, _ = counters.LoadOrStore(prefix, &counter)
	}

	counter := v.(*atomic.Uint64)
	return fmt.Sprintf("%s-%d", prefix, counter.Add(1))
}
