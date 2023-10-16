package memory_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory"
	"github.com/dogmatiq/persistencekit/kv"
)

func TestKeyValueStore(t *testing.T) {
	kv.RunTests(
		t,
		func(t *testing.T) kv.Store {
			return &KeyValueStore{}
		},
	)
}
