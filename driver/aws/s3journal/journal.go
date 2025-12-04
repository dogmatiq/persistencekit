package s3journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/internal/x/xerrors"
	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.BinaryJournal] that persists to an S3
// bucket.
//
// Each S3 object represents a journal [operation] such as [appendOperation] or
// [truncateOperation]. Objects are identified by a sequential ID, similar to a
// journal position.
type journ struct {
	client    *s3.Client
	onRequest func(any) []func(*s3.Options)

	// name is the journal name.
	name string

	// bucket is the name of the S3 bucket in which the journal's operations are
	// stored.
	bucket string

	// objectKeyPrefix is the string prepended to the key of each S3 object. It
	// includes the journal's name, allowing objects for multiple journals to be
	// stored in the same bucket.
	objectKeyPrefix string

	cache struct {
		// NextID is the ID of the next operation.
		NextID operationID

		// CompactionEndID is the (exclusive) end of the range of operations
		// that have been compacted (deleted).
		CompactionEndID operationID

		// Bounds are the bounds of the journal.
		Bounds journal.Interval

		// IsStale is true if the data in the cache is known to be out of date.
		IsStale bool
	}
}

func (j *journ) Name() string {
	return j.name
}

// Bounds returns the journal bounds after reloading the cache.
func (j *journ) Bounds(ctx context.Context) (journal.Interval, error) {
	err := j.refresh(ctx)
	return j.cache.Bounds, err
}

// refresh (re-)loads j.cache.
func (j *journ) refresh(ctx context.Context) (err error) {
	defer xerrors.Wrap(&err, "unable to refresh meta-data cache for the %q journal", j.Name())

	defer func() {
		j.cache.IsStale = err != nil
	}()

	req := &s3.ListObjectsV2Input{
		Bucket:  &j.bucket,
		Prefix:  &j.objectKeyPrefix,
		MaxKeys: aws.Int32(1),
	}

	for {
		list, err := awsx.Do(
			ctx,
			j.client.ListObjectsV2,
			j.onRequest,
			req,
		)
		if err != nil {
			return err
		}

		if len(list.Contents) == 0 {
			if j.cache.NextID != 0 {
				return fmt.Errorf("integrity error: non-fresh journal has no operations")
			}
			return nil
		}

		id, err := unmarshalOperationIDFromObjectKey(*list.Contents[0].Key)
		if err != nil {
			return err
		}

		// Loading the operation will also update the cache.
		if _, ok, err := j.loadOperationHead(ctx, id); ok || err != nil {
			return err
		}

		// We can't find the operation we just discovered by listing objects. We
		// can only assume it's been compacted and retry.
	}
}

func (j *journ) Get(ctx context.Context, pos journal.Position) (rec []byte, err error) {
	defer xerrors.Wrap(&err, "unable to get record at position %d of the %q journal", pos, j.Name())

	if pos < j.cache.Bounds.Begin {
		return nil, journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	op, err := j.findAppendOperation(ctx, pos, j.loadOperation)
	if err != nil {
		return nil, err
	}

	return op.Content, nil
}

func (j *journ) Range(
	ctx context.Context,
	pos journal.Position,
	fn journal.BinaryRangeFunc,
) (err error) {
	defer xerrors.Wrap(&err, "unable to range over records starting at position %d of the %q journal", pos, j.Name())

	if pos < j.cache.Bounds.Begin {
		return journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	op, err := j.findAppendOperation(ctx, pos, j.loadOperation)
	if err != nil {
		return err
	}

	if ok, err := fn(ctx, pos, op.Content); !ok || err != nil {
		return err
	}

	id := op.ID
	pos++

	for {
		id++

		op, ok, err := j.loadOperation(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		if op.Type != appendOperation {
			continue
		}

		if op.Bounds.End-1 != pos {
			return fmt.Errorf("integrity error: missing append operation for position %d", pos)
		}

		if ok, err := fn(ctx, pos, op.Content); !ok || err != nil {
			return err
		}

		pos++
	}
}

func (j *journ) Append(ctx context.Context, pos journal.Position, rec []byte) (err error) {
	defer xerrors.Wrap(&err, "unable to append record at position %d of the %q journal", pos, j.Name())

	return j.doOperation(
		ctx,
		func() (operation, bool, error) {
			if j.cache.Bounds.End != pos {
				return operation{}, false, journal.ConflictError{
					Journal:  j.Name(),
					Position: pos,
				}
			}

			return operation{
				Type:            appendOperation,
				CompactionEndID: j.cache.CompactionEndID,
				Bounds: journal.Interval{
					Begin: j.cache.Bounds.Begin,
					End:   pos + 1,
				},
				ContentType: "application/octet-stream",
				Content:     rec,
			}, true, nil
		},
	)
}

func (j *journ) Truncate(ctx context.Context, pos journal.Position) (err error) {
	defer xerrors.Wrap(&err, "unable to truncate records before position %d of the %q journal", pos, j.Name())

	if err := j.doOperation(
		ctx,
		func() (operation, bool, error) {
			if pos <= j.cache.Bounds.Begin {
				return operation{}, false, nil
			}

			if pos > j.cache.Bounds.End {
				return operation{}, false, errors.New("cannot truncate beyond the end of the journal")
			}

			return operation{
				Type:            truncateOperation,
				CompactionEndID: j.cache.CompactionEndID,
				Bounds: journal.Interval{
					Begin: pos,
					End:   j.cache.Bounds.End,
				},
			}, true, nil
		},
	); err != nil {
		return err
	}

	return j.compact(ctx)
}

func (j *journ) Close() error {
	return nil
}
