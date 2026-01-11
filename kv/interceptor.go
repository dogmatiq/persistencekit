package kv

import (
	"context"

	"github.com/dogmatiq/enginekit/x/xatomic"
)

// Interceptor defines functions that are invoked around keyspace operations.
type Interceptor[K, V any] struct {
	beforeOpen xatomic.Value[func(string) error]
	beforeSet  xatomic.Value[func(string, K, V) error]
	afterSet   xatomic.Value[func(string, K, V) error]
}

// BeforeOpen sets the function that is invoked before a [Keyspace] is opened.
func (i *Interceptor[K, V]) BeforeOpen(fn func(name string) error) {
	i.beforeOpen.Store(fn)
}

// BeforeSet sets the function that is invoked before a key/value pair is set.
func (i *Interceptor[K, V]) BeforeSet(fn func(keyspace string, k K, v V) error) {
	i.beforeSet.Store(fn)
}

// AfterSet sets the function that is invoked after a key/value pair is set.
func (i *Interceptor[K, V]) AfterSet(fn func(keyspace string, k K, v V) error) {
	i.afterSet.Store(fn)
}

// WithInterceptor returns a [Store] that invokes the functions defined by the
// given [Interceptor] when performing operations on s.
func WithInterceptor[K, V any](s Store[K, V], in *Interceptor[K, V]) Store[K, V] {
	if in == nil {
		return s
	}

	return &interceptedStore[K, V]{
		Next:        s,
		Interceptor: in,
	}
}

type interceptedStore[K, V any] struct {
	Next        Store[K, V]
	Interceptor *Interceptor[K, V]
}

func (s *interceptedStore[K, V]) Open(ctx context.Context, name string) (Keyspace[K, V], error) {
	if fn := s.Interceptor.beforeOpen.Load(); fn != nil {
		if err := fn(name); err != nil {
			return nil, err
		}
	}

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		return nil, err
	}

	return &interceptedKeyspace[K, V]{
		Next:        next,
		keyspace:    next.Name(),
		Interceptor: s.Interceptor,
	}, nil
}

type interceptedKeyspace[K, V any] struct {
	Next        Keyspace[K, V]
	keyspace    string
	Interceptor *Interceptor[K, V]
}

func (ks *interceptedKeyspace[K, V]) Name() string {
	return ks.Next.Name()
}

func (ks *interceptedKeyspace[K, V]) Get(ctx context.Context, k K) (V, error) {
	return ks.Next.Get(ctx, k)
}

func (ks *interceptedKeyspace[K, V]) Has(ctx context.Context, k K) (bool, error) {
	return ks.Next.Has(ctx, k)
}

func (ks *interceptedKeyspace[K, V]) Set(ctx context.Context, k K, v V) error {
	if fn := ks.Interceptor.beforeSet.Load(); fn != nil {
		if err := fn(ks.keyspace, k, v); err != nil {
			return err
		}
	}

	if err := ks.Next.Set(ctx, k, v); err != nil {
		return err
	}

	if fn := ks.Interceptor.afterSet.Load(); fn != nil {
		if err := fn(ks.keyspace, k, v); err != nil {
			return err
		}
	}

	return nil
}

func (ks *interceptedKeyspace[K, V]) Range(ctx context.Context, fn RangeFunc[K, V]) error {
	return ks.Next.Range(ctx, fn)
}

func (ks *interceptedKeyspace[K, V]) Close() error {
	return ks.Next.Close()
}
