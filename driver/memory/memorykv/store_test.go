package memorykv_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestStore(t *testing.T) {
	kv.RunTests(
		t,
		func(t *testing.T) kv.Store {
			return &Store{}
		},
	)
}

func BenchmarkStore(b *testing.B) {
	kv.RunBenchmarks(
		b,
		func(b *testing.B) kv.Store {
			return &Store{}
		},
	)
}
