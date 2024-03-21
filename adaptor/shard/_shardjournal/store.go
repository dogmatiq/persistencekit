package partitionedjournal

import (
	"context"
	"strings"

	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that partitions journals across
// multiple underlying stores.
type Store struct {
	// Partitions is the store used to persist partitioning information.
	Partitions journal.Store
}

// type Partitioner interface {
// 	SelectStore(ctx context.Context) (string, error)
// 	OpenJournal(ctx context.Context, name string) (journal.Journal, error)
// 	OpenJournalOnStore(ctx context.Context, store, name string) (journal.Journal, error)
// }

// type PartitionCache interface {
// 	SetStore(ctx context.Context, journal, store string) error
// 	GetStore(ctx context.Context, journal string) (string, bool, error)
// }

// Open returns the journal with the given name.
func (s *Store) Open(ctx context.Context, name string) (journal.Journal, error) {
	j, err := s.Partitions.Open(ctx, join(name, "association"))
	if err != nil {
		return nil, err
	}
	defer j.Close()

	_, end, err := j.Bounds(ctx)
	if err != nil {
		return nil, err
	}

	if end > 0 {
	}

	err != nil{}

	// partitionsName := join(name, "partitions")
	// storeName, ok, err := s.PartitionCache.GetStore(ctx, partitionsName)
	// if err != nil {
	// 	return nil, err
	// }

	// if !ok {
	// 	var err error
	// 	key, err = s.Partitioner.SelectStore(ctx)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// if len(storeKey) == 0 {
	// }

	// panic("not implemented")
}

func (s *Store) open(ctx context.Context, name string) (journal.Journal, error) {
	storeName, ok, err := s.PartitionCache.GetStore(ctx, name)
	if err != nil {
		return nil, err
	}

	if !ok {
		var err error
		storeName, err = s.Partitioner.SelectStore(ctx)
		if err != nil {
			return nil, err
		}

		err = s.PartitionCache.SetStore(ctx, name, storeName)
	}

	store, err := s.Partitioner.StoreByName(ctx, storeName)
	if err != nil {
		return nil, err
	}

	panic("not implemented")
}

// func (s *Store) openPartitionTable(ctx context.Context, name string) (journal.Journal, error) {
// 	for _, store := range s.Stores {
// 		j, ok, err := tryOpenPartitionTable(ctx, store, name)
// 		if err != nil {
// 			return nil,err
// 		}

// 			continue
// 		}
// 		} else if ok {
// 			return j, nil
// 		} else {
// 			j.Close()
// 		}
// 	}

// 	return nil, errs
// }

// func tryOpenPartitionTable(
// 	ctx context.Context,
// 	store journal.Store,
// 	name string,
// ) (j journal.Journal, ok bool, err error) {
// 	name = join(name, "partition-table")

// 	j, err = store.Open(ctx, name)
// 	if err != nil {
// 		return nil, false, err
// 	}

// 	_, end, err := j.Bounds(ctx)
// 	if err != nil {
// 		j.Close()
// 		return nil, false, err
// 	}

// 	return j, end > 0, nil
// }

func join(names ...string) string {
	// separate name parts using ASCII "unit separator" control character.
	return strings.Join(names, "\x1f")
}
