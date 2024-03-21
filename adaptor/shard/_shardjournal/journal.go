package partitionedjournal

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
)

type journ struct {
	stores map[string]journal.Store
	name   string
}

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	panic("not implemented")
}

func (j *journ) Get(ctx context.Context, pos journal.Position) (rec []byte, err error) {
	panic("not implemented")
}

func (j *journ) Range(ctx context.Context, pos journal.Position, fn journal.RangeFunc) error {
	panic("not implemented")
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	panic("not implemented")
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
	panic("not implemented")
}

func (j *journ) Close() error {
	panic("not implemented")
}
