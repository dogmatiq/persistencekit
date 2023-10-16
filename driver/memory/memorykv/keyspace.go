package memorykv

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/dogmatiq/persistencekit/kv"
)

// state is the in-memory state of a keyspace.
type state struct {
	sync.RWMutex
	Values map[string][]byte
}

// journ is an implementation of [kv.Keyspace] that manipulates a keyspace's
// in-memory [state].
type keyspace struct {
	name      string
	state     *state
	beforeSet func(ks string, k, v []byte) error
	afterSet  func(ks string, k, v []byte) error
}

func (h *keyspace) Get(ctx context.Context, k []byte) (v []byte, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	return slices.Clone(h.state.Values[string(k)]), ctx.Err()
}

func (h *keyspace) Has(ctx context.Context, k []byte) (ok bool, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	_, ok = h.state.Values[string(k)]
	return ok, ctx.Err()
}

func (h *keyspace) Set(ctx context.Context, k, v []byte) error {
	if h.state == nil {
		panic("keyspace is closed")
	}

	v = slices.Clone(v)

	h.state.Lock()
	defer h.state.Unlock()

	if h.beforeSet != nil {
		if err := h.beforeSet(h.name, k, v); err != nil {
			return err
		}
	}

	if len(v) == 0 {
		delete(h.state.Values, string(k))
	} else {
		if h.state.Values == nil {
			h.state.Values = map[string][]byte{}
		}

		h.state.Values[string(k)] = v
	}

	if h.afterSet != nil {
		if err := h.afterSet(h.name, k, v); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (h *keyspace) Range(
	ctx context.Context,
	fn kv.RangeFunc,
) error {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	values := maps.Clone(h.state.Values)
	h.state.RUnlock()

	for k, v := range values {
		ok, err := fn(ctx, []byte(k), slices.Clone(v))
		if !ok || err != nil {
			return err
		}
	}

	return nil
}

func (h *keyspace) Close() error {
	if h.state == nil {
		return errors.New("keyspace is already closed")
	}

	h.state = nil

	return nil
}
