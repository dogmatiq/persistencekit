package test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/dogmatiq/persistencekit/kv"
)

// KeyValueStore is an implementation of [kv.Store] that stores keyspaces in
// memory.
type KeyValueStore struct {
	keyspaces sync.Map // map[string]*keyspaceState

	BeforeSetHook func(ks string, k, v []byte) error
	AfterSetHook  func(ks string, k, v []byte) error
}

// Open returns the keyspace with the given name.
func (s *KeyValueStore) Open(ctx context.Context, name string) (kv.Keyspace, error) {
	state, ok := s.keyspaces.Load(name)

	if !ok {
		state, _ = s.keyspaces.LoadOrStore(
			name,
			&keyspaceState{},
		)
	}

	return &keyspaceHandle{
		state:         state.(*keyspaceState),
		beforeSetHook: s.BeforeSetHook,
		afterSetHook:  s.AfterSetHook,
	}, ctx.Err()
}

type keyspaceState struct {
	sync.RWMutex
	Values map[string][]byte
}

type keyspaceHandle struct {
	name          string
	state         *keyspaceState
	beforeSetHook func(ks string, k, v []byte) error
	afterSetHook  func(ks string, k, v []byte) error
}

func (h *keyspaceHandle) Get(ctx context.Context, k []byte) (v []byte, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	return slices.Clone(h.state.Values[string(k)]), ctx.Err()
}

func (h *keyspaceHandle) Has(ctx context.Context, k []byte) (ok bool, err error) {
	if h.state == nil {
		panic("keyspace is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	_, ok = h.state.Values[string(k)]
	return ok, ctx.Err()
}

func (h *keyspaceHandle) Set(ctx context.Context, k, v []byte) error {
	if h.state == nil {
		panic("keyspace is closed")
	}

	v = slices.Clone(v)

	h.state.Lock()
	defer h.state.Unlock()

	if h.beforeSetHook != nil {
		if err := h.beforeSetHook(h.name, k, v); err != nil {
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

	if h.afterSetHook != nil {
		if err := h.afterSetHook(h.name, k, v); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (h *keyspaceHandle) Range(
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

func (h *keyspaceHandle) Close() error {
	if h.state == nil {
		return errors.New("keyspace is already closed")
	}

	h.state = nil

	return nil
}
