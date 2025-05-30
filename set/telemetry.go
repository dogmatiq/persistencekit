package set

import (
	"context"

	"github.com/dogmatiq/persistencekit/internal/telemetry"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WithTelemetry returns a [BinaryStore] that adds telemetry to s.
func WithTelemetry(
	s BinaryStore,
	p trace.TracerProvider,
	m metric.MeterProvider,
	l log.LoggerProvider,
) BinaryStore {
	return &instrumentedStore{
		Next: s,
		Telemetry: telemetry.Provider{
			TracerProvider: p,
			MeterProvider:  m,
			LoggerProvider: l,
		},
	}
}

// instrumentedStore is a decorator that adds instrumentation to a [BinaryStore].
type instrumentedStore struct {
	Next      BinaryStore
	Telemetry telemetry.Provider
}

// Open returns the set with the given name.
func (s *instrumentedStore) Open(ctx context.Context, name string) (BinarySet, error) {
	telem := s.Telemetry.Recorder(
		"github.com/dogmatiq/persistencekit/set",
		telemetry.Type("store", s.Next),
		telemetry.String("handle", telemetry.HandleID()),
		telemetry.String("name", name),
	)

	set := &instrumentedSet{
		Telemetry:  telem,
		OpenCount:  telem.UpDownCounter("open_sets", "{set}", "The number of sets that are currently open."),
		ValueCount: telem.Counter("values", "{value}", "The number of values that have been operated upon."),
		ValueBytes: telem.Counter("value_bytes", "By", "The cumulative size of the values that have been operated upon."),
		ValueSizes: telem.Histogram("value_sizes", "By", "The sizes of the values that have been operated upon."),
	}

	ctx, span := telem.StartSpan(ctx, "set.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		telem.Error(ctx, "set.open.error", err)
		return nil, err
	}

	set.Next = next

	set.OpenCount(ctx, 1)
	set.Telemetry.Info(ctx, "set.open.ok", "opened set")

	return set, nil
}

type instrumentedSet struct {
	Next      BinarySet
	Telemetry *telemetry.Recorder

	OpenCount  telemetry.Instrument[int64]
	ValueCount telemetry.Instrument[int64]
	ValueBytes telemetry.Instrument[int64]
	ValueSizes telemetry.Instrument[int64]
}

func (s *instrumentedSet) Name() string {
	return s.Next.Name()
}

func (s *instrumentedSet) Has(ctx context.Context, v []byte) (bool, error) {
	size := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.has",
		telemetry.Binary("value", v),
		telemetry.Int("value_size", size),
	)
	defer span.End()

	s.ValueCount(ctx, 1, telemetry.WriteDirection)
	s.ValueBytes(ctx, size, telemetry.WriteDirection)
	s.ValueSizes(ctx, size, telemetry.WriteDirection)

	ok, err := s.Next.Has(ctx, v)
	if err != nil {
		s.Telemetry.Error(ctx, "set.has.error", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("value_present", ok),
	)

	if ok {
		s.Telemetry.Info(ctx, "set.has.ok", "value is present in set")
	} else {
		s.Telemetry.Info(ctx, "set.has.ok", "value is not present in set")
	}

	return ok, nil
}

func (s *instrumentedSet) Add(ctx context.Context, v []byte) error {
	size := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.add",
		telemetry.Binary("value", v),
		telemetry.Int("value_size", size),
	)
	defer span.End()

	s.ValueCount(ctx, 1, telemetry.WriteDirection)
	s.ValueBytes(ctx, size, telemetry.WriteDirection)
	s.ValueSizes(ctx, size, telemetry.WriteDirection)

	if err := s.Next.Add(ctx, v); err != nil {
		s.Telemetry.Error(ctx, "set.add.error", err)
		return err
	}

	s.Telemetry.Info(ctx, "set.add.ok", "added value to set")

	return nil
}

func (s *instrumentedSet) TryAdd(ctx context.Context, v []byte) (bool, error) {
	size := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.try_add",
		telemetry.Binary("value", v),
		telemetry.Int("value_size", size),
	)
	defer span.End()

	s.ValueCount(ctx, 1, telemetry.WriteDirection)
	s.ValueBytes(ctx, size, telemetry.WriteDirection)
	s.ValueSizes(ctx, size, telemetry.WriteDirection)

	ok, err := s.Next.TryAdd(ctx, v)
	if err != nil {
		s.Telemetry.Error(ctx, "set.try_add.error", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("value_added", ok),
	)

	if ok {
		s.Telemetry.Info(ctx, "set.try_add.ok", "value was added to set")
	} else {
		s.Telemetry.Info(ctx, "set.try_add.ok", "value was already present in set")
	}

	return ok, nil
}

func (s *instrumentedSet) Remove(ctx context.Context, v []byte) error {
	size := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.remove",
		telemetry.Binary("value", v),
		telemetry.Int("value_size", size),
	)
	defer span.End()

	s.ValueCount(ctx, 1, telemetry.WriteDirection)
	s.ValueBytes(ctx, size, telemetry.WriteDirection)
	s.ValueSizes(ctx, size, telemetry.WriteDirection)

	if err := s.Next.Remove(ctx, v); err != nil {
		s.Telemetry.Error(ctx, "set.remove.error", err)
		return err
	}

	s.Telemetry.Info(ctx, "set.remove.ok", "removed value from set")

	return nil
}

func (s *instrumentedSet) TryRemove(ctx context.Context, v []byte) (bool, error) {
	size := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.try_remove",
		telemetry.Binary("value", v),
		telemetry.Int("value_size", size),
	)
	defer span.End()

	s.ValueCount(ctx, 1, telemetry.WriteDirection)
	s.ValueBytes(ctx, size, telemetry.WriteDirection)
	s.ValueSizes(ctx, size, telemetry.WriteDirection)

	ok, err := s.Next.TryRemove(ctx, v)
	if err != nil {
		s.Telemetry.Error(ctx, "set.try_remove.error", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("value_removed", ok),
	)

	if ok {
		s.Telemetry.Info(ctx, "set.try_remove.ok", "value was removed from set")
	} else {
		s.Telemetry.Info(ctx, "set.try_remove.ok", "value was not present in set")
	}

	return ok, nil
}

func (s *instrumentedSet) Close() error {
	if s.Next == nil {
		// If the resource has already been closed don't do anything at all,
		// even log a warning, because we want to allow the caller to defer
		// closing for safety _and_ close explicitly elsewhere for error
		// checking.
		return nil
	}

	ctx, span := s.Telemetry.StartSpan(context.Background(), "set.close")
	defer span.End()

	defer func() {
		s.Next = nil
		s.OpenCount(ctx, -1)
	}()

	if err := s.Next.Close(); err != nil {
		s.Telemetry.Error(ctx, "set.close.error", err)
		return err
	}

	s.Telemetry.Info(ctx, "set.close.ok", "closed set")

	return nil
}
