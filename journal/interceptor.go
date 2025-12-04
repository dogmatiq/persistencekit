package journal

import (
	"context"

	"github.com/dogmatiq/persistencekit/internal/x/xatomic"
)

// Interceptor defines functions that are invoked around journal operations.
type Interceptor[T any] struct {
	beforeOpen   xatomic.Value[func(string) error]
	beforeAppend xatomic.Value[func(string, T) error]
	afterAppend  xatomic.Value[func(string, T) error]
}

// BeforeOpen sets the function that is invoked before a [Journal] is opened.
func (i *Interceptor[T]) BeforeOpen(fn func(name string) error) {
	i.beforeOpen.Store(fn)
}

// BeforeAppend sets the function that is invoked before a record is appended to
// the [Journal].
func (i *Interceptor[T]) BeforeAppend(fn func(journal string, rec T) error) {
	i.beforeAppend.Store(fn)
}

// AfterAppend sets the function that is invoked after a record is appended to
// the [Journal].
func (i *Interceptor[T]) AfterAppend(fn func(journal string, rec T) error) {
	i.afterAppend.Store(fn)
}

// WithInterceptor returns a [Store] that invokes the functions defined by the
// given [Interceptor] when performing operations on s.
func WithInterceptor[T any](s Store[T], in *Interceptor[T]) Store[T] {
	if in == nil {
		return s
	}

	return &interceptedStore[T]{
		Next:        s,
		Interceptor: in,
	}
}

type interceptedStore[T any] struct {
	Next        Store[T]
	Interceptor *Interceptor[T]
}

func (s *interceptedStore[T]) Open(ctx context.Context, name string) (Journal[T], error) {
	if fn := s.Interceptor.beforeOpen.Load(); fn != nil {
		if err := fn(name); err != nil {
			return nil, err
		}
	}

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &interceptedJournal[T]{
		Next:        next,
		journal:     next.Name(),
		Interceptor: s.Interceptor,
	}, nil
}

type interceptedJournal[T any] struct {
	Next        Journal[T]
	journal     string
	Interceptor *Interceptor[T]
}

func (j *interceptedJournal[T]) Name() string {
	return j.Next.Name()
}

func (j *interceptedJournal[T]) Bounds(ctx context.Context) (Interval, error) {
	return j.Next.Bounds(ctx)
}

func (j *interceptedJournal[T]) Get(ctx context.Context, pos Position) (T, error) {
	return j.Next.Get(ctx, pos)
}

func (j *interceptedJournal[T]) Range(ctx context.Context, pos Position, fn RangeFunc[T]) error {
	return j.Next.Range(ctx, pos, fn)
}

func (j *interceptedJournal[T]) Append(ctx context.Context, pos Position, rec T) error {
	if fn := j.Interceptor.beforeAppend.Load(); fn != nil {
		if err := fn(j.journal, rec); err != nil {
			return err
		}
	}

	if err := j.Next.Append(ctx, pos, rec); err != nil {
		return err
	}

	if fn := j.Interceptor.afterAppend.Load(); fn != nil {
		if err := fn(j.journal, rec); err != nil {
			return err
		}
	}

	return nil
}

func (j *interceptedJournal[T]) Truncate(ctx context.Context, pos Position) error {
	return j.Next.Truncate(ctx, pos)
}

func (j *interceptedJournal[T]) Close() error {
	return j.Next.Close()
}
