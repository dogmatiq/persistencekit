package typedjournal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/persistencekit/journal/typedjournal"
)

func TestStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	store := Store[int, jsonMarshaler[int]]{
		Store: &memoryjournal.Store{},
	}

	j, err := store.Open(ctx, "<name>")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer j.Close()

	if err := j.Append(ctx, 0, 100); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := j.Append(ctx, 1, 101); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	fn := func(ctx context.Context, pos journal.Position, rec int) (bool, error) {
		expect := int(pos) + 100
		if rec != expect {
			t.Fatalf("unexpected value at position %d: got %d, want %d", pos, rec, expect)
		}
		return true, nil
	}

	if err := j.Range(ctx, 0, fn); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for pos := journal.Position(0); pos < 2; pos++ {
		rec, err := j.Get(ctx, pos)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fn(ctx, pos, rec)
	}
}

type jsonMarshaler[R any] struct{}

func (m jsonMarshaler[R]) Marshal(rec R) ([]byte, error) {
	return json.Marshal(rec)
}

func (m jsonMarshaler[R]) Unmarshal(data []byte) (R, error) {
	var rec R
	return rec, json.Unmarshal(data, &rec)
}
