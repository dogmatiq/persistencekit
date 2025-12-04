package xtesting

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// UniqueName returns a unique name with the given prefix.
func UniqueName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.NewString())
}

var counters sync.Map

// SequentialName returns a unique name with the given prefix.
func SequentialName(prefix string) string {
	v, ok := counters.Load(prefix)
	if !ok {
		var counter atomic.Uint64
		v, _ = counters.LoadOrStore(prefix, &counter)
	}

	counter := v.(*atomic.Uint64)
	return fmt.Sprintf("%s-%d", prefix, counter.Add(1))
}
