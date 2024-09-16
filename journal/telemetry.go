package journal

import (
	"context"
	"errors"
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

// Open returns the journal with the given name.
func (s *instrumentedStore) Open(ctx context.Context, name string) (BinaryJournal, error) {
	r := s.Telemetry.Recorder(
		"github.com/dogmatiq/persistencekit",
		"journal",
		telemetry.Type("store", s.Next),
		telemetry.String("handle", telemetry.HandleID()),
		telemetry.String("name", name),
	)

	ctx, span := r.StartSpan(ctx, "journal.open")
	defer span.End()

	next, err := s.Next.Open(ctx, name)
	if err != nil {
		span.Error("could not open journal", err)
		return nil, err
	}

	j := &instrumentedJournal{
		Next:      next,
		Telemetry: r,
		OpenCount: r.Int64UpDownCounter(
			"open_journals",
			metric.WithDescription("The number of journals that are currently open."),
			metric.WithUnit("{journal}"),
		),
		ConflictCount: r.Int64Counter(
			"conflicts",
			metric.WithDescription("The number of times appending a record to the journal has failed due to a optimistic-concurrency conflict."),
			metric.WithUnit("{conflict}"),
		),
		DataIO: r.Int64Counter(
			"io",
			metric.WithDescription("The cumulative size of the journal records that have been read and written."),
			metric.WithUnit("By"),
		),
		RecordIO: r.Int64Counter(
			"record.io",
			metric.WithDescription("The number of journal records that have been read and written."),
			metric.WithUnit("{record}"),
		),
		RecordSize: r.Int64Histogram(
			"record.size",
			metric.WithDescription("The sizes of the journal records that have been read and written."),
			metric.WithUnit("By"),
		),
	}

	j.OpenCount.Add(ctx, 1)
	span.Debug("opened journal")

	return j, nil
}

type instrumentedJournal struct {
	Next      BinaryJournal
	Telemetry *telemetry.Recorder

	OpenCount     metric.Int64UpDownCounter
	ConflictCount metric.Int64Counter
	DataIO        metric.Int64Counter
	RecordIO      metric.Int64Counter
	RecordSize    metric.Int64Histogram
}

func (j *instrumentedJournal) Bounds(ctx context.Context) (bounds Interval, err error) {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.bounds",
	)
	defer span.End()

	bounds, err = j.Next.Bounds(ctx)
	if err != nil {
		span.Error("could not fetch journal bounds", err)
		return Interval{}, err
	}

	span.SetAttributes(
		telemetry.Int("begin", bounds.Begin),
		telemetry.Int("end", bounds.End),
	)

	span.Debug("fetched journal bounds")

	return bounds, nil
}

func (j *instrumentedJournal) Get(ctx context.Context, pos Position) ([]byte, error) {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.get",
		telemetry.Int("position", pos),
	)
	defer span.End()

	rec, err := j.Next.Get(ctx, pos)
	if err != nil {
		span.Error("could not fetch journal record", err)
		return nil, err
	}

	size := int64(len(rec))

	span.SetAttributes(
		telemetry.Int("record_size", size),
	)

	j.DataIO.Add(ctx, size, telemetry.ReadDirection)
	j.RecordIO.Add(ctx, 1, telemetry.ReadDirection)
	j.RecordSize.Record(ctx, size, telemetry.ReadDirection)

	span.Debug("fetched single journal record")

	return rec, nil
}

func (j *instrumentedJournal) Range(
	ctx context.Context,
	begin Position,
	fn BinaryRangeFunc,
) error {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.range",
		telemetry.Int("range_start", begin),
	)
	defer span.End()

	var (
		first, count Position
		totalSize    int64
		brokeLoop    bool
	)

	span.Debug("reading journal records")

	err := j.Next.Range(
		ctx,
		begin,
		func(ctx context.Context, pos Position, rec []byte) (bool, error) {
			if count == 0 {
				first = pos
			}
			count++

			size := int64(len(rec))
			totalSize += size

			j.DataIO.Add(ctx, size, telemetry.ReadDirection)
			j.RecordIO.Add(ctx, 1, telemetry.ReadDirection)
			j.RecordSize.Record(ctx, size, telemetry.ReadDirection)

			ok, err := fn(ctx, pos, rec)
			if ok || err != nil {
				return ok, err
			}

			brokeLoop = true
			return false, nil
		},
	)

	if count != 0 {
		span.SetAttributes(
			telemetry.Int("range_start", first),
			telemetry.Int("range_stop", first+count-1),
		)
	}

	span.SetAttributes(
		telemetry.Int("record_read", count),
		telemetry.Int("bytes_read", totalSize),
		telemetry.Bool("reached_end", !brokeLoop && err == nil),
	)

	if err != nil {
		span.Error("could not read journal records", err)
		return err
	}

	span.Debug("completed reading journal records")

	return nil
}

func (j *instrumentedJournal) Append(ctx context.Context, end Position, rec []byte) error {
	size := int64(len(rec))

	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.append",
		telemetry.Int("position", end),
		telemetry.Int("record_size", size),
	)
	defer span.End()

	j.DataIO.Add(ctx, size, telemetry.WriteDirection)
	j.RecordIO.Add(ctx, 1, telemetry.WriteDirection)
	j.RecordSize.Record(ctx, size, telemetry.WriteDirection)

	err := j.Next.Append(ctx, end, rec)
	if err != nil {
		span.Error("unable to append journal record", err)

		if errors.Is(err, ErrConflict) {
			span.SetAttributes(
				telemetry.Bool("conflict", true),
			)

			j.ConflictCount.Add(ctx, 1)
		}

		return err
	}

	span.Debug("journal record appended")

	return nil
}

func (j *instrumentedJournal) Truncate(ctx context.Context, end Position) error {
	ctx, span := j.Telemetry.StartSpan(
		ctx,
		"journal.truncate",
		telemetry.Int("retained_position", end),
	)
	defer span.End()

	if err := j.Next.Truncate(ctx, end); err != nil {
		span.Error("unable to truncate journal", err)
		return err
	}

	span.Debug("truncated oldest journal records")

	return nil
}

func (j *instrumentedJournal) Close() error {
	ctx, span := j.Telemetry.StartSpan(context.Background(), "journal.close")
	defer span.End()

	if j.Next == nil {
		span.Warn("journal is already closed")
		return nil
	}

	defer func() {
		j.Next = nil
		j.OpenCount.Add(ctx, -1)
	}()

	if err := j.Next.Close(); err != nil {
		span.Error("could not close journal", err)
		return err
	}

	span.Debug("closed journal")

	return nil
}
