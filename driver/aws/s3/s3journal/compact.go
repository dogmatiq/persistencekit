package s3journal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/internal/x/xerrors"
)

// compact deletes any operations that occurred before the beginning of the
// journal (after truncation).
func (j *journ) compact(ctx context.Context) (err error) {
	defer xerrors.Wrap(&err, "unable to compact operations in the %q journal", j.Name())

	if j.cache.IsStale {
		panic("cannot compact with a stale cache")
	}

	if j.cache.NextID == 0 {
		// The journal is fresh, there's nothing to compact.
		return nil
	}

	if j.cache.Bounds.Begin == 0 {
		// No records have been truncated, so there's no operations to compact.
		return nil
	}

	begin := j.cache.CompactionEndID
	end := j.cache.NextID - 1 // never compact the last remaining operation

	if !j.cache.Bounds.IsEmpty() {
		// Find the append operation that produced the record at the begin
		// position so we don't compact it.
		op, err := j.findAppendOperation(
			ctx,
			j.cache.Bounds.Begin,
			j.loadOperationHead,
		)
		if err != nil {
			return err
		}

		if op.ID < end {
			end = op.ID
		}
	}

	if begin == end {
		// Everything that can be compacted has already been compacted.
		return nil
	}

	// maxBatch is the maximum number of objects that can be deleted in a
	// single DeleteObjects request, as per the S3 API.
	const maxBatch = 1000

	// batch is the list of objects to delete in the next DeleteObjects request.
	var batch []types.ObjectIdentifier

	for {
		// Fill the batch with the next set of undeleted objects.
		for len(batch) < maxBatch && begin < end {
			batch = append(
				batch,
				types.ObjectIdentifier{
					Key: j.objectKeyForOperationID(begin),
				},
			)
			begin++
		}

		// If there's nothing in the batch, we're done!
		if len(batch) == 0 {
			return j.doOperation(
				ctx,
				func() (operation, bool, error) {
					if end <= j.cache.CompactionEndID {
						// Another process has compacted the same records and
						// already recorded that fact, so we bail.
						return operation{}, false, nil
					}

					return operation{
						Type:            compactedOperation,
						CompactionEndID: end,
						Bounds:          j.cache.Bounds,
					}, true, nil
				},
			)
		}

		// Delete the objects in the batch.
		res, err := awsx.Do(
			ctx,
			j.client.DeleteObjects,
			j.onRequest,
			&s3.DeleteObjectsInput{
				Bucket: &j.bucket,
				Delete: &types.Delete{
					Objects: batch,
					Quiet:   aws.Bool(true),
				},
			},
		)
		if err != nil {
			return err
		}

		// Empty the batch, but keep the underlying array.
		batch = batch[:0]

		// Re-add any failed deletions from this iteration to the batch so that
		// they are retried on the next iteration.
		for _, err := range res.Errors {
			if *err.Code != "NoSuchKey" {
				batch = append(
					batch,
					types.ObjectIdentifier{
						Key:       err.Key,
						VersionId: err.VersionId,
					},
				)
			}
		}
	}
}
