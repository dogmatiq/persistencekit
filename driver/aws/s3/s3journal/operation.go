package s3journal

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/internal/x/xerrors"
	"github.com/dogmatiq/persistencekit/journal"
)

type (
	operationID   uint64
	operationType string
)

const (
	// appendOperation is a type of [operation] that appends a record to the
	// journal.
	appendOperation = "append"

	// truncateOperation is a type of [operation] that truncates the oldest
	// records from the journal.
	truncateOperation = "truncate"

	// compactedOperation is a type of [operation] that records the fact that
	// some truncated records have been compacted.
	compactedOperation = "compacted"
)

const (
	operationIDMetaData   = "id"
	operationTypeMetaData = "type"
	beginPositionMetaData = "begin"
	endPositionMetaData   = "end"
)

// operation is some action that modifies the journal.
type operation struct {
	// ID is the sequential ID of the operation.
	ID operationID

	// Type is the type of operation being performed.
	Type operationType

	// CompactionEndID is the (exclusive) end of the range of operations that
	// have been compacted (deleted).
	CompactionEndID operationID

	// Bounds is describes the journal bounds as they are after the operation.
	Bounds journal.Interval

	// ContentType is the MIME type of the content.
	ContentType string

	// Content is the operation-type-specific content of the operation.
	Content []byte
}

// doOperation performs an operation on the journal.
func (j *journ) doOperation(
	ctx context.Context,
	fn func() (operation, bool, error),
) error {
	for {
		if j.cache.IsStale {
			if err := j.refresh(ctx); err != nil {
				return err
			}
		}

		op, ok, err := fn()
		if !ok || err != nil {
			return err
		}

		if op.ID != 0 {
			panic("operation should not set its own ID")
		}

		if op.CompactionEndID < j.cache.CompactionEndID {
			panic("operation attempts to un-compact the journal")
		}

		if op.Type == "" {
			panic("operation type is empty")
		}

		if len(op.Content) != 0 && op.ContentType == "" {
			panic("operation contains content but content-type is empty")
		}

		if op.Bounds.Begin > j.cache.Bounds.End {
			panic("operation has invalid bounds")
		}

		if op.Bounds.Begin < j.cache.Bounds.Begin {
			panic("operation attempts to un-truncate the journal")
		}

		if op.Bounds.End < j.cache.Bounds.End {
			panic("operation attempts to un-append to the journal")
		}

		op.ID = j.cache.NextID

		req := &s3.PutObjectInput{
			Bucket:      &j.bucket,
			Key:         j.objectKeyForOperationID(op.ID),
			IfNoneMatch: aws.String("*"),
			Metadata: map[string]string{
				operationIDMetaData:   marshalUint64(op.ID),
				operationTypeMetaData: string(op.Type),
				beginPositionMetaData: marshalUint64(op.Bounds.Begin),
				endPositionMetaData:   marshalUint64(op.Bounds.End),
			},
		}

		if op.ContentType != "" {
			req.ContentType = aws.String(op.ContentType)
			req.ContentLength = aws.Int64(int64(len(op.Content)))
			req.Body = s3x.NewReadSeeker(op.Content)
		}

		_, err = awsx.Do(
			ctx,
			j.client.PutObject,
			j.onRequest,
			req,
		)

		if err == nil {
			j.updateCache(op)
			return nil
		}

		j.cache.IsStale = true

		fmt.Println(req.Key)

		if !s3x.IsConflict(err) {
			return err
		}
	}
}

// loadOperation returns the operation with the given ID.
func (j *journ) loadOperation(ctx context.Context, id operationID) (op operation, ok bool, err error) {
	defer xerrors.Wrap(&err, "unable to load operation %d of the %q journal", id, j.Name())

	res, err := awsx.Do(
		ctx,
		j.client.GetObject,
		j.onRequest,
		&s3.GetObjectInput{
			Bucket: &j.bucket,
			Key:    j.objectKeyForOperationID(id),
		},
	)
	if err != nil {
		return operation{}, false, s3x.IgnoreNotExists(err)
	}
	defer res.Body.Close()

	op, err = unmarshalOperation(res)
	if err != nil {
		return operation{}, false, err
	}

	j.updateCache(op)

	return op, true, err
}

func (j *journ) loadOperationHead(ctx context.Context, id operationID) (operation, bool, error) {
	res, err := awsx.Do(
		ctx,
		j.client.HeadObject,
		j.onRequest,
		&s3.HeadObjectInput{
			Bucket: &j.bucket,
			Key:    j.objectKeyForOperationID(id),
		},
	)
	if err != nil {
		return operation{}, false, s3x.IgnoreNotExists(err)
	}

	op, err := unmarshalOperationHead(res)
	if err != nil {
		return operation{}, false, err
	}

	j.updateCache(op)

	return op, true, err
}

// updateCache updates the cache based on an operation that has been
// (partially) loaded and possibly not seen before.
func (j *journ) updateCache(op operation) {
	if op.ID >= j.cache.NextID {
		// If we loaded an operation that's newer than what we've seen in the
		// cache, assume it's the latest operation.
		j.cache.NextID = op.ID + 1
		j.cache.CompactionEndID = op.CompactionEndID
		j.cache.Bounds = op.Bounds
		j.cache.IsStale = false
	}
}

// findAppendOperation finds the journal operation that appended the record at
// the given position.
func (j *journ) findAppendOperation(
	ctx context.Context,
	pos journal.Position,
	loader func(context.Context, operationID) (operation, bool, error),
) (op operation, err error) {
	// Fail fast if the position has been truncated. It doesn't matter if the
	// cache is stale because the lower bounds can only increase.
	if pos < j.cache.Bounds.Begin {
		return operation{}, journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	// Start with the operation with an ID equal to the position we're looking
	// for, if possible. If all operations were [appendOperation], this will be
	// the operation we're looking for.
	id := max(
		operationID(pos),
		j.cache.CompactionEndID,
	)

	for {
		op, ok, err := loader(ctx, id)
		if err != nil {
			return operation{}, err
		}

		if !ok {
			// The operation does not exist, therefore pos must be beyond the
			// end of the journal.
			return operation{}, journal.RecordNotFoundError{
				Journal:  j.Name(),
				Position: pos,
			}
		}

		if pos < op.Bounds.Begin {
			// We found a record that indicates pos has been truncated (which we
			// didn't catch by inspecting our cache earlier).
			return operation{}, journal.RecordNotFoundError{
				Journal:  j.Name(),
				Position: pos,
			}
		}

		if op.Bounds.End <= pos {
			// The operation was appended before pos existed, keep looking.
			//
			// TODO(jmalloc): Can we optimize this; something better than linear
			// scan? We could keep track of additional meta-data if necessary.
			id++
			continue
		}

		if op.Type != appendOperation || op.Bounds.End > pos+1 {
			return operation{}, fmt.Errorf("integrity error: missing append operation for position %d", pos)
		}

		return op, nil
	}
}

// objectKeyForOperationID returns the S3 object key for the operation with the
// given ID.
//
// The keys are arranged such that listing the S3 objects will return them in
// the reverse order of their IDs. This allows querying the "end" of the journal
// using an S3 operation.
func (j *journ) objectKeyForOperationID(id operationID) *string {
	return aws.String(j.objectKeyPrefix + marshalOperationIDForObjectKey(id))
}

func marshalOperationIDForObjectKey(id operationID) string {
	inverse := ^uint64(id)
	hex := strconv.FormatUint(inverse, 16)
	hex = strings.Repeat("0", 16-len(hex)) + hex
	return hex
}

func unmarshalOperationIDFromObjectKey(key string) (operationID, error) {
	if len(key) < 16 {
		return 0, fmt.Errorf("integrity error: operation object key %q is too short", key)
	}

	hex := key[len(key)-16:]
	inverse, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("integrity error: operation object key %q does not a hexadecimal suffix: %w", key, err)
	}

	return operationID(^inverse), nil
}

func unmarshalOperation(res *s3.GetObjectOutput) (operation, error) {
	op, err := unmarshalOperationMetaData(res.Metadata)
	if err != nil {
		return operation{}, err
	}

	if res.ContentType != nil && *res.ContentType != "" {
		op.ContentType = *res.ContentType

		op.Content, err = io.ReadAll(res.Body)
		if err != nil {
			return operation{}, fmt.Errorf("unable to read journal record content: %w", err)
		}
	}

	return op, nil
}

func unmarshalOperationHead(res *s3.HeadObjectOutput) (operation, error) {
	op, err := unmarshalOperationMetaData(res.Metadata)
	if err != nil {
		return operation{}, err
	}

	if res.ContentType != nil {
		op.ContentType = *res.ContentType
	}

	return op, nil
}

func unmarshalOperationMetaData(meta map[string]string) (op operation, err error) {
	op.ID, err = unmarshalUint64[operationID](meta, operationIDMetaData)
	if err != nil {
		return operation{}, err
	}

	op.Type, err = unmarshalString[operationType](meta, operationTypeMetaData)
	if err != nil {
		return operation{}, err
	}

	op.Bounds.Begin, err = unmarshalUint64[journal.Position](meta, beginPositionMetaData)
	if err != nil {
		return operation{}, err
	}

	op.Bounds.End, err = unmarshalUint64[journal.Position](meta, endPositionMetaData)
	if err != nil {
		return operation{}, err
	}

	return op, nil
}

func unmarshalString[T ~string](meta map[string]string, key string) (T, error) {
	str, ok := meta[key]
	if !ok {
		return "", fmt.Errorf("integrity error: %q meta-data is missing", key)
	}

	if str == "" {
		return "", fmt.Errorf("integrity error: %q meta-data is present but has an empty value", key)
	}

	return T(str), nil
}

func marshalUint64[T ~uint64](n T) string {
	return strconv.FormatUint(uint64(n), 10)
}

func unmarshalUint64[T ~uint64](meta map[string]string, key string) (T, error) {
	str, ok := meta[key]
	if !ok {
		return 0, fmt.Errorf("integrity error: %q meta-data is missing", key)
	}

	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("integrity error: %q meta-data is not a valid %T: %w", key, T(0), err)
	}

	return T(n), nil
}
