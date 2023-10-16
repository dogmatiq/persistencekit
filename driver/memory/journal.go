package memory

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/dogmatiq/persistencekit/journal"
)

// JournalStore is an implementation of [journal.Store] that stores journals in
// memory.
type JournalStore struct {
	journals sync.Map // map[string]*journalState

	BeforeOpenHook   func(name string) error
	BeforeAppendHook func(name string, rec []byte) error
	AfterAppendHook  func(name string, rec []byte) error
}

// Open returns the journal with the given name.
func (s *JournalStore) Open(ctx context.Context, name string) (journal.Journal, error) {
	if s.BeforeOpenHook != nil {
		if err := s.BeforeOpenHook(name); err != nil {
			return nil, err
		}
	}

	state, ok := s.journals.Load(name)

	if !ok {
		state, _ = s.journals.LoadOrStore(
			name,
			&journalState{},
		)
	}

	return &journalHandle{
		name:             name,
		state:            state.(*journalState),
		beforeAppendHook: s.BeforeAppendHook,
		afterAppendHook:  s.AfterAppendHook,
	}, ctx.Err()
}

// NewJournal returns a new standalone journal.
func NewJournal() journal.Journal {
	return &journalHandle{
		state: &journalState{},
	}
}

// journalState stores the underlying state of a journal.
type journalState struct {
	sync.RWMutex
	Begin, End journal.Position
	Records    [][]byte
}

// journalHandle is an implementation of [journal.Journal] that accesses
// in-memory journal state.
type journalHandle struct {
	name             string
	state            *journalState
	beforeAppendHook func(name string, rec []byte) error
	afterAppendHook  func(name string, rec []byte) error
}

func (h *journalHandle) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	if h.state == nil {
		panic("journal is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	return h.state.Begin, h.state.End, ctx.Err()
}

func (h *journalHandle) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	if h.state == nil {
		panic("journal is closed")
	}

	h.state.RLock()
	defer h.state.RUnlock()

	if pos < h.state.Begin || pos >= h.state.End {
		return nil, journal.ErrNotFound
	}

	return slices.Clone(h.state.Records[pos-h.state.Begin]), ctx.Err()
}

func (h *journalHandle) Range(
	ctx context.Context,
	begin journal.Position,
	fn journal.RangeFunc,
) error {
	if h.state == nil {
		panic("journal is closed")
	}

	h.state.RLock()
	first := h.state.Begin
	records := h.state.Records
	h.state.RUnlock()

	if first > begin {
		return journal.ErrNotFound
	}

	start := begin - first
	for i, rec := range records[start:] {
		v := start + journal.Position(i)
		ok, err := fn(ctx, v, slices.Clone(rec))
		if !ok || err != nil {
			return err
		}
		begin++
	}

	return ctx.Err()
}

func (h *journalHandle) Append(ctx context.Context, end journal.Position, rec []byte) error {
	if h.state == nil {
		panic("journal is closed")
	}

	rec = slices.Clone(rec)

	h.state.Lock()
	defer h.state.Unlock()

	if h.beforeAppendHook != nil {
		if err := h.beforeAppendHook(h.name, rec); err != nil {
			return err
		}
	}

	switch {
	case end < h.state.End:
		return journal.ErrConflict
	case end == h.state.End:
		h.state.Records = append(h.state.Records, rec)
		h.state.End++
	default:
		panic("position out of range, this causes undefined behavior in a 'real' journal implementation")
	}

	if h.afterAppendHook != nil {
		if err := h.afterAppendHook(h.name, rec); err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (h *journalHandle) Truncate(ctx context.Context, end journal.Position) error {
	if h.state == nil {
		panic("journal is closed")
	}

	h.state.Lock()
	defer h.state.Unlock()

	if end > h.state.End {
		panic("position out of range, this causes undefined behavior in a real journal implementation")
	}

	if end > h.state.Begin {
		h.state.Records = h.state.Records[end-h.state.Begin:]
		h.state.Begin = end
	}

	return ctx.Err()
}

func (h *journalHandle) Close() error {
	if h.state == nil {
		return errors.New("journal is already closed")
	}

	h.state = nil

	return nil
}
