package memorykv

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/internal/clone"
	"github.com/dogmatiq/persistencekit/kv"
)

// state is the in-memory state of a keyspace.
type state[C comparable, V any] struct {
	sync.RWMutex
	Values map[C]V
}

// keyspace is an implementation of [kv.BinaryKeyspace] that manipulates a
// keyspace's in-memory [state].
type keyspace[K, V any, C comparable] struct {
	name         string
	state        *state[C, V]
	beforeSet    func(ks string, k K, v V) error
	afterSet     func(ks string, k K, v V) error
	marshalKey   func(K) C
	unmarshalKey func(C) K
}

func (h *keyspace[K, V, C]) Name() string {
	return h.name
}

func (h *keyspace[K, V, C]) Get(ctx context.Context, k K) (v V, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	c := h.marshalKey(k)
	return clone.Clone(h.state.Values[c]), ctx.Err()
}

func (h *keyspace[K, V, C]) Has(ctx context.Context, k K) (ok bool, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	c := h.marshalKey(k)
	_, ok = h.state.Values[c]
	return ok, ctx.Err()
}

func (h *keyspace[K, V, C]) Set(ctx context.Context, k K, v V) error {
	if h.state == nil {
		panic("keyspace is closed")
	}

	v = clone.Clone(v)

	h.state.Lock()
	defer h.state.Unlock()

	if h.beforeSet != nil {
		if err := h.beforeSet(h.name, k, v); err != nil {
			return err
		}
	}

	c := h.marshalKey(k)

	if reflect.ValueOf(v).IsZero() {
		delete(h.state.Values, c)
	} else {
		if h.state.Values == nil {
			h.state.Values = map[C]V{}
		}

		h.state.Values[c] = v
	}

	if h.afterSet != nil {
		if err := h.afterSet(h.name, k, v); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (h *keyspace[K, V, C]) Range(ctx context.Context, fn kv.RangeFunc[K, V]) error {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	values := maps.Clone(h.state.Values)
	h.state.RUnlock()

	for c, v := range values {
		k := h.unmarshalKey(c)
		ok, err := fn(ctx, k, clone.Clone(v))
		if !ok || err != nil {
			return err
		}
	}

	return nil
}

func (h *keyspace[K, V, C]) Close() error {
	if h.state == nil {
		return errors.New("keyspace is already closed")
	}

	h.state = nil

	return nil
}
