package typedjournal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/persistencekit/journal/typedjournal"
)

func TestStore(t *testing.T) {
	mstore := &memoryjournal.Store{}
	tstore := StoreOf[record, JSONMarshaler[record]]{
		Store: mstore,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	j, err := tstore.Open(ctx, "<name>")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer j.Close()

	if err := j.Append(ctx, 0, record{Value: "<value-0>"}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := j.Append(ctx, 1, record{Value: "<value-1>"}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	fn := func(ctx context.Context, pos journal.Position, rec record) (bool, error) {
		expect := fmt.Sprintf("<value-%d>", pos)
		if rec.Value != expect {
			t.Fatalf("unexpected value at position %d: got %q, want %q", pos, rec.Value, expect)
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

type record struct {
	Value string
}

type JSONMarshaler[R any] struct {
}

func (m JSONMarshaler[R]) Marshal(rec R) ([]byte, error) {
	return json.Marshal(rec)
}

func (m JSONMarshaler[R]) Unmarshal(data []byte) (R, error) {
	var rec R
	return rec, json.Unmarshal(data, &rec)
}
