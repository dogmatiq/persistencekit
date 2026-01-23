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
	Items map[C]item[V]
}

type item[V any] struct {
	Value    V
	Revision kv.Revision
}

// keyspace is an implementation of [kv.BinaryKeyspace] that manipulates a
// keyspace's in-memory [state].
type keyspace[K, V any, C comparable] struct {
	name         string
	state        *state[C, V]
	marshalKey   func(K) C
	unmarshalKey func(C) K
}

func (ks *keyspace[K, V, C]) Name() string {
	return ks.name
}

func (ks *keyspace[K, V, C]) Get(ctx context.Context, k K) (v V, r kv.Revision, err error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	defer ks.state.RUnlock()

	c := ks.marshalKey(k)

	if i, ok := ks.state.Items[c]; ok {
		v = clone.Clone(i.Value)
		r = i.Revision
	}

	return v, r, ctx.Err()
}

func (ks *keyspace[K, V, C]) Has(ctx context.Context, k K) (ok bool, err error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	defer ks.state.RUnlock()

	c := ks.marshalKey(k)
	_, ok = ks.state.Items[c]
	return ok, ctx.Err()
}

func (ks *keyspace[K, V, C]) Set(ctx context.Context, k K, v V, r kv.Revision) error {
	return ks.set(ctx, k, v, &r)
}

func (ks *keyspace[K, V, C]) SetUnconditional(ctx context.Context, k K, v V) error {
	return ks.set(ctx, k, v, nil)
}

func (ks *keyspace[K, V, C]) set(ctx context.Context, k K, v V, r *kv.Revision) error {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	v = clone.Clone(v)

	ks.state.Lock()
	defer ks.state.Unlock()

	c := ks.marshalKey(k)
	i := ks.state.Items[c]

	if r != nil && *r != i.Revision {
		return kv.ConflictError[K]{
			Keyspace: ks.name,
			Key:      k,
			Revision: *r,
		}
	}

	if reflect.ValueOf(v).IsZero() {
		delete(ks.state.Items, c)
		return ctx.Err()
	}

	if ks.state.Items == nil {
		ks.state.Items = map[C]item[V]{}
	}

	ks.state.Items[c] = item[V]{
		Value:    v,
		Revision: i.Revision + 1,
	}

	return ctx.Err()
}

func (ks *keyspace[K, V, C]) Range(ctx context.Context, fn kv.RangeFunc[K, V]) error {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	items := maps.Clone(ks.state.Items)
	ks.state.RUnlock()

	for c, i := range items {
		ok, err := fn(
			ctx,
			ks.unmarshalKey(c),
			clone.Clone(i.Value),
			i.Revision,
		)
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
