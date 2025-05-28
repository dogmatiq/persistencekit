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

func (ks *keyspace[K, V, C]) Name() string {
	return ks.name
}

func (ks *keyspace[K, V, C]) Get(ctx context.Context, k K) (v V, err error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	defer ks.state.RUnlock()

	c := ks.marshalKey(k)
	return clone.Clone(ks.state.Values[c]), ctx.Err()
}

func (ks *keyspace[K, V, C]) Has(ctx context.Context, k K) (ok bool, err error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	defer ks.state.RUnlock()

	c := ks.marshalKey(k)
	_, ok = ks.state.Values[c]
	return ok, ctx.Err()
}

func (ks *keyspace[K, V, C]) Set(ctx context.Context, k K, v V) error {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	v = clone.Clone(v)

	ks.state.Lock()
	defer ks.state.Unlock()

	if ks.beforeSet != nil {
		if err := ks.beforeSet(ks.name, k, v); err != nil {
			return err
		}
	}

	c := ks.marshalKey(k)

	if reflect.ValueOf(v).IsZero() {
		delete(ks.state.Values, c)
	} else {
		if ks.state.Values == nil {
			ks.state.Values = map[C]V{}
		}

		ks.state.Values[c] = v
	}

	if ks.afterSet != nil {
		if err := ks.afterSet(ks.name, k, v); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (ks *keyspace[K, V, C]) Range(ctx context.Context, fn kv.RangeFunc[K, V]) error {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	values := maps.Clone(ks.state.Values)
	ks.state.RUnlock()

	for c, v := range values {
		k := ks.unmarshalKey(c)
		ok, err := fn(ctx, k, clone.Clone(v))
		if !ok || err != nil {
			return err
		}
	}

	return nil
}

func (ks *keyspace[K, V, C]) Close() error {
	if ks.state == nil {
		return errors.New("keyspace is already closed")
	}

	ks.state = nil

	return nil
}
