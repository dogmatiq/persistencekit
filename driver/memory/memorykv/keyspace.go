package memorykv

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
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
	Value V
	Token []byte
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

func (ks *keyspace[K, V, C]) Get(ctx context.Context, k K) (v V, t []byte, err error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	defer ks.state.RUnlock()

	c := ks.marshalKey(k)

	if i, ok := ks.state.Items[c]; ok {
		v = clone.Clone(i.Value)
		t = slices.Clone(i.Token)
	}

	return v, t, ctx.Err()
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

func (ks *keyspace[K, V, C]) Set(ctx context.Context, k K, v V, t []byte) ([]byte, error) {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	v = clone.Clone(v)
	t = slices.Clone(t)

	ks.state.Lock()
	defer ks.state.Unlock()

	c := ks.marshalKey(k)
	i := ks.state.Items[c]

	if !bytes.Equal(t, i.Token) {
		return nil, kv.ConflictError[K]{
			Keyspace: ks.name,
			Key:      k,
			Token:    t,
		}
	}

	if reflect.ValueOf(v).IsZero() {
		delete(ks.state.Items, c)
		return nil, ctx.Err()
	}

	t = nextToken(t)

	if ks.state.Items == nil {
		ks.state.Items = map[C]item[V]{}
	}

	ks.state.Items[c] = item[V]{
		Value: v,
		Token: t,
	}

	return t, ctx.Err()
}

func (ks *keyspace[K, V, C]) Range(ctx context.Context, fn kv.RangeFunc[K, V]) error {
	if ks.state == nil {
		panic("keyspace is closed")
	}

	ks.state.RLock()
	items := maps.Clone(ks.state.Items)
	ks.state.RUnlock()

	for c, i := range items {
		k := ks.unmarshalKey(c)
		ok, err := fn(
			ctx,
			k,
			clone.Clone(i.Value),
			slices.Clone(i.Token),
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

// nextToken returns the next concurrency token to use for a value after t.
// It may mutate t itself.
func nextToken(t []byte) []byte {
	n := len(t) - 1

	if n >= 0 && t[n] < 0xff {
		t[n]++
	} else {
		t = append(t, 0x00)
	}

	return t
}
