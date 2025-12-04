package set

import (
	"context"

	"github.com/dogmatiq/persistencekit/internal/x/xatomic"
)

// Interceptor defines functions that are invoked around set operations.
type Interceptor[T any] struct {
	beforeOpen   xatomic.Value[func(string) error]
	beforeAdd    xatomic.Value[func(string, T) error]
	afterAdd     xatomic.Value[func(string, T) error]
	beforeRemove xatomic.Value[func(string, T) error]
	afterRemove  xatomic.Value[func(string, T) error]
}

// BeforeOpen sets the function that is invoked before a [Set] is opened.
func (i *Interceptor[T]) BeforeOpen(fn func(name string) error) {
	i.beforeOpen.Store(fn)
}

// BeforeAdd sets the function that is invoked before a member is added to the
// [Set].
func (i *Interceptor[T]) BeforeAdd(fn func(set string, v T) error) {
	i.beforeAdd.Store(fn)
}

// AfterAdd sets the function that is invoked after a member is added to the
// [Set].
func (i *Interceptor[T]) AfterAdd(fn func(set string, v T) error) {
	i.afterAdd.Store(fn)
}

// BeforeRemove sets the function that is invoked before a member is removed
// from the [Set].
func (i *Interceptor[T]) BeforeRemove(fn func(set string, v T) error) {
	i.beforeRemove.Store(fn)
}

// AfterRemove sets the function that is invoked after a member is removed from
// the [Set].
func (i *Interceptor[T]) AfterRemove(fn func(set string, v T) error) {
	i.afterRemove.Store(fn)
}

// WithInterceptor returns a [Store] that invokes the functions defined
// by the given [Interceptor] when performing operations on s.
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

func (s *interceptedStore[T]) Open(ctx context.Context, name string) (Set[T], error) {
	if fn := s.Interceptor.beforeOpen.Load(); fn != nil {
		if err := fn(name); err != nil {
			return nil, err
		}
	}

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &interceptedSet[T]{
		Next:        next,
		set:         next.Name(),
		Interceptor: s.Interceptor,
	}, nil
}

type interceptedSet[T any] struct {
	Next        Set[T]
	set         string
	Interceptor *Interceptor[T]
}

func (s *interceptedSet[T]) Name() string {
	return s.Next.Name()
}

func (s *interceptedSet[T]) Has(ctx context.Context, v T) (bool, error) {
	return s.Next.Has(ctx, v)
}

func (s *interceptedSet[T]) Add(ctx context.Context, v T) error {
	if fn := s.Interceptor.beforeAdd.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return err
		}
	}

	if err := s.Next.Add(ctx, v); err != nil {
		return err
	}

	if fn := s.Interceptor.afterAdd.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return err
		}
	}

	return nil
}

func (s *interceptedSet[T]) TryAdd(ctx context.Context, v T) (bool, error) {
	if fn := s.Interceptor.beforeAdd.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return false, err
		}
	}

	added, err := s.Next.TryAdd(ctx, v)
	if err != nil {
		return false, err
	}

	if fn := s.Interceptor.afterAdd.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return false, err
		}
	}

	return added, nil
}

func (s *interceptedSet[T]) Remove(ctx context.Context, v T) error {
	if fn := s.Interceptor.beforeRemove.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return err
		}
	}

	if err := s.Next.Remove(ctx, v); err != nil {
		return err
	}

	if fn := s.Interceptor.afterRemove.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return err
		}
	}

	return nil
}

func (s *interceptedSet[T]) TryRemove(ctx context.Context, v T) (bool, error) {
	if fn := s.Interceptor.beforeRemove.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return false, err
		}
	}

	removed, err := s.Next.TryRemove(ctx, v)
	if err != nil {
		return false, err
	}

	if fn := s.Interceptor.afterRemove.Load(); fn != nil {
		if err := fn(s.set, v); err != nil {
			return false, err
		}
	}

	return removed, nil
}

func (s *interceptedSet[T]) Range(ctx context.Context, fn RangeFunc[T]) error {
	return s.Next.Range(ctx, fn)
}

func (s *interceptedSet[T]) Close() error {
	return s.Next.Close()
}
