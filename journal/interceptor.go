package journal

import (
	"context"
	"sync/atomic"
)

// Interceptor defines functions that are invoked around journal operations.
type Interceptor[T any] struct {
	beforeOpen   atomic.Pointer[func(string) error]
	beforeAppend atomic.Pointer[func(string, T) error]
	afterAppend  atomic.Pointer[func(string, T) error]
}

// BeforeOpen sets the function that is invoked before a [Journal] is opened.
func (i *Interceptor[T]) BeforeOpen(fn func(name string) error) {
	setOpenFn(&i.beforeOpen, fn)
}

// BeforeAppend sets the function that is invoked before a record is appended to
// the [Journal].
func (i *Interceptor[T]) BeforeAppend(fn func(journal string, rec T) error) {
	setRecordFn(&i.beforeAppend, fn)
}

// AfterAppend sets the function that is invoked after a record is appended to
// the [Journal].
func (i *Interceptor[T]) AfterAppend(fn func(journal string, rec T) error) {
	setRecordFn(&i.afterAppend, fn)
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

func setOpenFn(dst *atomic.Pointer[func(string) error], fn func(string) error) {
	if fn == nil {
		dst.Store(nil)
		return
	}

	dst.Store(&fn)
}

func setRecordFn[T any](dst *atomic.Pointer[func(string, T) error], fn func(string, T) error) {
	if fn == nil {
		dst.Store(nil)
		return
	}

	dst.Store(&fn)
}

type interceptedStore[T any] struct {
	Next        Store[T]
	Interceptor *Interceptor[T]
}

func (s *interceptedStore[T]) Open(ctx context.Context, name string) (Journal[T], error) {
	if fn := s.Interceptor.beforeOpenFn(); fn != nil {
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
	if fn := j.Interceptor.beforeAppendFn(); fn != nil {
		if err := fn(j.journal, rec); err != nil {
			return err
		}
	}

	if err := j.Next.Append(ctx, pos, rec); err != nil {
		return err
	}

	if fn := j.Interceptor.afterAppendFn(); fn != nil {
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

func (i *Interceptor[T]) beforeOpenFn() func(string) error {
	if i == nil {
		return nil
	}

	if fn := i.beforeOpen.Load(); fn != nil {
		return *fn
	}

	return nil
}

func (i *Interceptor[T]) beforeAppendFn() func(string, T) error {
	if i == nil {
		return nil
	}

	if fn := i.beforeAppend.Load(); fn != nil {
		return *fn
	}

	return nil
}

func (i *Interceptor[T]) afterAppendFn() func(string, T) error {
	if i == nil {
		return nil
	}

	if fn := i.afterAppend.Load(); fn != nil {
		return *fn
	}

	return nil
}
