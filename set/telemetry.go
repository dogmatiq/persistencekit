package set

import (
	"context"
	"log/slog"

	"github.com/dogmatiq/persistencekit/internal/telemetry"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// WithTelemetry returns a [BinaryStore] that adds telemetry to s.
func WithTelemetry(
	s BinaryStore,
	p trace.TracerProvider,
	m metric.MeterProvider,
	l *slog.Logger,
) BinaryStore {
	return &instrumentedStore{
		Next: s,
		Telemetry: telemetry.Provider{
			TracerProvider: p,
			MeterProvider:  m,
			Logger:         l,
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
	r := s.Telemetry.Recorder(
		"github.com/dogmatiq/persistencekit",
		"set",
		telemetry.Type("store", s.Next),
		telemetry.String("handle", telemetry.HandleID()),
		telemetry.String("name", name),
	)

	ctx, span := r.StartSpan(ctx, "set.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		span.Error("could not open set", err)
		return nil, err
	}

	set := &instrumentedSet{
		Next:      next,
		Telemetry: r,
		OpenCount: r.Int64UpDownCounter(
			"open_sets",
			metric.WithDescription("The number of sets that are currently open."),
			metric.WithUnit("{set}"),
		),
		DataIO: r.Int64Counter(
			"io",
			metric.WithDescription("The cumulative size of the values that have been operated upon."),
			metric.WithUnit("By"),
		),
		ValueIO: r.Int64Counter(
			"value.io",
			metric.WithDescription("The number of values that have been operated upon."),
			metric.WithUnit("{value}"),
		),
		ValueSize: r.Int64Histogram(
			"value.size",
			metric.WithDescription("The sizes of the values that have been operated upon."),
			metric.WithUnit("By"),
		),
	}

	set.OpenCount.Add(ctx, 1)
	span.Debug("opened set")

	return set, nil
}

type instrumentedSet struct {
	Next      BinarySet
	Telemetry *telemetry.Recorder

	OpenCount metric.Int64UpDownCounter
	DataIO    metric.Int64Counter
	ValueIO   metric.Int64Counter
	ValueSize metric.Int64Histogram
}

func (s *instrumentedSet) Name() string {
	return s.Next.Name()
}

func (s *instrumentedSet) Has(ctx context.Context, v []byte) (bool, error) {
	valueSize := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.has",
		telemetry.If(
			isShortASCII(v),
			telemetry.String("value", string(v)),
		),
		telemetry.Int("value_size", valueSize),
	)
	defer span.End()

	s.DataIO.Add(ctx, valueSize, telemetry.WriteDirection)
	s.ValueSize.Record(ctx, valueSize, telemetry.WriteDirection)

	ok, err := s.Next.Has(ctx, v)
	if err != nil {
		span.Error("could not check for presence of value", err)
		return false, err
	}

	s.ValueIO.Add(ctx, 1, telemetry.ReadDirection)

	span.SetAttributes(
		telemetry.Bool("value_present", ok),
	)

	span.Debug("checked for presence of value")

	return ok, nil
}

func (s *instrumentedSet) Add(ctx context.Context, v []byte) error {
	valueSize := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.add",
		telemetry.If(
			isShortASCII(v),
			telemetry.String("value", string(v)),
		),
		telemetry.Int("value_size", valueSize),
	)
	defer span.End()

	s.DataIO.Add(ctx, valueSize, telemetry.WriteDirection)
	s.ValueIO.Add(ctx, 1, telemetry.WriteDirection)
	s.ValueSize.Record(ctx, valueSize, telemetry.WriteDirection)

	if err := s.Next.Add(ctx, v); err != nil {
		span.Error("could add value to set", err)
		return err
	}

	span.Debug("added value")

	return nil
}

func (s *instrumentedSet) TryAdd(ctx context.Context, v []byte) (bool, error) {
	valueSize := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.try_add",
		telemetry.If(
			isShortASCII(v),
			telemetry.String("value", string(v)),
		),
		telemetry.Int("value_size", valueSize),
	)
	defer span.End()

	s.DataIO.Add(ctx, valueSize, telemetry.WriteDirection)
	s.ValueIO.Add(ctx, 1, telemetry.WriteDirection)
	s.ValueSize.Record(ctx, valueSize, telemetry.WriteDirection)

	ok, err := s.Next.TryAdd(ctx, v)
	if err != nil {
		span.Error("could not add value to set", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("value_added", ok),
	)

	if ok {
		span.Debug("added value")
	} else {
		span.Debug("value was already present")
	}

	return ok, nil
}

func (s *instrumentedSet) Remove(ctx context.Context, v []byte) error {
	valueSize := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.remove",
		telemetry.If(
			isShortASCII(v),
			telemetry.String("value", string(v)),
		),
		telemetry.Int("value_size", valueSize),
	)
	defer span.End()

	s.DataIO.Add(ctx, valueSize, telemetry.WriteDirection)
	s.ValueIO.Add(ctx, 1, telemetry.WriteDirection)
	s.ValueSize.Record(ctx, valueSize, telemetry.WriteDirection)

	if err := s.Next.Remove(ctx, v); err != nil {
		span.Error("could not remove value from set", err)
		return err
	}

	span.Debug("removed value")

	return nil
}

func (s *instrumentedSet) TryRemove(ctx context.Context, v []byte) (bool, error) {
	valueSize := int64(len(v))

	ctx, span := s.Telemetry.StartSpan(
		ctx,
		"set.try_remove",
		telemetry.If(
			isShortASCII(v),
			telemetry.String("value", string(v)),
		),
		telemetry.Int("value_size", valueSize),
	)
	defer span.End()

	s.DataIO.Add(ctx, valueSize, telemetry.WriteDirection)
	s.ValueIO.Add(ctx, 1, telemetry.WriteDirection)
	s.ValueSize.Record(ctx, valueSize, telemetry.WriteDirection)

	ok, err := s.Next.TryRemove(ctx, v)
	if err != nil {
		span.Error("could not remove value from set", err)
		return false, err
	}

	span.SetAttributes(
		telemetry.Bool("value_removed", ok),
	)

	if ok {
		span.Debug("removed value")
	} else {
		span.Debug("value was not present")
	}

	return ok, nil
}

func (s *instrumentedSet) Close() error {
	ctx, span := s.Telemetry.StartSpan(context.Background(), "set.close")
	defer span.End()

	if s.Next == nil {
		span.Warn("set is already closed")
		return nil
	}

	defer func() {
		s.Next = nil
		s.OpenCount.Add(ctx, -1)
	}()

	if err := s.Next.Close(); err != nil {
		span.Error("could not close set", err)
		return err
	}

	span.Debug("closed set")

	return nil
}

// isShortASCII returns true if k is a non-empty ASCII string short enough that
// it may be included as a telemetry attribute.
func isShortASCII(k []byte) bool {
	if len(k) == 0 || len(k) > 128 {
		return false
	}

	for _, octet := range k {
		if octet < ' ' || octet > '~' {
			return false
		}
	}

	return true
}
