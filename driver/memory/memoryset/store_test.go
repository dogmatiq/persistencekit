package memoryset_test

import (
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/set"
)

func TestStore(t *testing.T) {
	set.RunTests(
		t,
		&BinaryStore{},
	)
}

func BenchmarkStore(b *testing.B) {
	set.RunBenchmarks(
		b,
		&BinaryStore{},
	)
}
