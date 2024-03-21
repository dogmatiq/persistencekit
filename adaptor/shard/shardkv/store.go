package shardkv

import (
	"context"
	"fmt"

	"github.com/dogmatiq/persistencekit/adaptor/shard/internal/hrw"
	"github.com/dogmatiq/persistencekit/kv"
)

type StoreProvider[K, V any] interface {
	StoreIDs(ctx context.Context) ([]string, error)
	StoreByID(ctx context.Context, id string) (kv.Store[K, V], error)
}

type store[K, V any] struct {
	provider StoreProvider[K, V]
	shards   kv.Keyspace[string, string]
}

func (s *store[K, V]) Open(
	ctx context.Context,
	name string,
) (kv.Keyspace[K, V], error) {
	id, err := s.shards.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("cannot load store ID for %q keyspace: %w", name, err)
	}

	if id != "" {
		store, err := s.provider.StoreByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("cannot load obtain store with ID %q: %w", id, err)
		}
		return store.Open(ctx, name)
	}

	storeIDs, err := s.provider.StoreIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot to load available store IDs: %w", err)
	}

	for _, id := range hrw.Rank(storeIDs, name) {
	}
}

type keyspace[K, V any] struct {
}

// if err := s.shards.Set(ctx, name, id); err != nil {
// 	return nil, fmt.Errorf("cannot save store ID for %q keyspace: %w", name, err)
// }
