package test_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/test"
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
