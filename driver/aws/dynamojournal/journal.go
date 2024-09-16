package dynamojournal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.BinaryJournal] that persists to a
// DynamoDB table.
type journ struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	name     *types.AttributeValueMemberS
	position *types.AttributeValueMemberN
	record   *types.AttributeValueMemberB

	boundsReq   dynamodb.QueryInput
	getReq      dynamodb.GetItemInput
	rangeReq    dynamodb.QueryInput
	appendReq   dynamodb.PutItemInput
	truncateReq dynamodb.UpdateItemInput
	deleteReq   dynamodb.DeleteItemInput
}

func (j *journ) Bounds(ctx context.Context) (bounds journal.Interval, err error) {
	end, empty, err := j.upperBound(ctx)
	if err != nil {
		return journal.Interval{}, err
	}

	if empty {
		return journal.Interval{
			Begin: end,
			End:   end,
		}, nil
	}

	begin, err := j.lowerBound(ctx, end)
	if err != nil {
		return journal.Interval{}, err
	}

	return journal.Interval{
		Begin: begin,
		End:   end,
	}, nil
}

// upperBound returns the (exclusive) upper bound of the records in the journal.
//
// If empty is true, the journal is either truly empty or all records have been
// truncated, and therefore the bounds are [end, end).
func (j *journ) upperBound(ctx context.Context) (end journal.Position, empty bool, err error) {
	*j.boundsReq.ScanIndexForward = false

	empty = true

	if _, err := dynamox.QueryOne(
		ctx,
		j.Client,
		j.OnRequest,
		&j.boundsReq,
		func(ctx context.Context, item map[string]types.AttributeValue) error {
			pos, err := unmarshalPosition(item)
			if err != nil {
				return err
			}

			// The [begin, end) range is half-open, so the end position is the
			// one AFTER the most recent record.
			end = pos + 1

			// If the most recent record has been truncated, the journal is
			// effectively empty with bounds of [end, end).
			empty, err = isTruncated(item)
			return err
		},
	); err != nil {
		return 0, false, fmt.Errorf("unable to query last journal record: %w", err)
	}

	return end, empty, nil
}

// lowerBound returns the (inclusive) lower bound of the records in the journal.
func (j *journ) lowerBound(ctx context.Context, end journal.Position) (begin journal.Position, err error) {
	*j.boundsReq.ScanIndexForward = true

	limit := j.boundsReq.Limit
	defer func() { j.boundsReq.Limit = limit }()

	j.boundsReq.Limit = aws.Int32(5) // arbitrary, but we don't want to load too many

	if err := dynamox.QueryRange(
		ctx,
		j.Client,
		j.OnRequest,
		&j.boundsReq,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			pos, err := unmarshalPosition(item)
			if err != nil {
				return false, err
			}

			truncated, err := isTruncated(item)
			if err != nil {
				return false, err
			}

			// If we found a non-truncated record, it must be the lower bound.
			if !truncated {
				begin = pos
				return false, nil
			}

			// Otherwise, we found a truncated record.
			//
			// If it's the same record we used to identify the upper bound then
			// all records are truncated.
			if pos+1 == end {
				begin = end
				return false, nil
			}

			// Otherwise we've found a record that has been marked as truncated,
			// but we know there are records after it, so we clean it up as we
			// go.
			return true, j.deleteRecord(ctx, pos)
		},
	); err != nil {
		return 0, fmt.Errorf("unable to query first journal record: %w", err)
	}

	return begin, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	j.position.Value = marshalPosition(pos)

	out, err := awsx.Do(
		ctx,
		j.Client.GetItem,
		j.OnRequest,
		&j.getReq,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get journal record: %w", err)
	}
	if out.Item == nil {
		return nil, journal.ErrNotFound
	}

	truncated, err := isTruncated(out.Item)
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, journal.ErrNotFound
	}

	rec, err := dynamox.AttrAs[*types.AttributeValueMemberB](out.Item, recordAttr)
	if err != nil {
		return nil, err
	}

	return rec.Value, nil
}

func (j *journ) Range(
	ctx context.Context,
	pos journal.Position,
	fn journal.BinaryRangeFunc,
) error {
	j.position.Value = marshalPosition(pos)
	expectPos := pos

	err := dynamox.QueryRange(
		ctx,
		j.Client,
		j.OnRequest,
		&j.rangeReq,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			pos, err := unmarshalPosition(item)
			if err != nil {
				return false, err
			}

			if pos != expectPos {
				return false, journal.ErrNotFound
			}

			expectPos++

			rec, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, recordAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, pos, rec.Value)
		},
	)

	if err == journal.ErrNotFound {
		return err
	} else if err != nil {
		return fmt.Errorf("unable to range over journal records: %w", err)
	} else if expectPos == pos {
		return journal.ErrNotFound
	}

	return nil
}

func (j *journ) Append(ctx context.Context, pos journal.Position, rec []byte) error {
	j.position.Value = marshalPosition(pos)
	j.record.Value = rec

	if _, err := awsx.Do(
		ctx,
		j.Client.PutItem,
		j.OnRequest,
		&j.appendReq,
	); err != nil {
		if errors.As(err, new(*types.ConditionalCheckFailedException)) {
			return journal.ErrConflict
		}
		return fmt.Errorf("unable to put journal record: %w", err)
	}

	return nil
}

func (j *journ) Truncate(ctx context.Context, pos journal.Position) error {
	bounds, err := j.Bounds(ctx)
	if err != nil {
		return err
	}

	if pos > bounds.End {
		return errors.New("cannot truncate beyond the end of the journal")
	}

	for p := bounds.Begin; p < pos; p++ {
		j.position.Value = marshalPosition(p)

		var err error
		if p+1 == bounds.End {
			err = j.markRecordAsTruncated(ctx, p)
		} else {
			err = j.deleteRecord(ctx, p)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (j *journ) markRecordAsTruncated(ctx context.Context, pos journal.Position) error {
	j.position.Value = marshalPosition(pos)

	if _, err := awsx.Do(
		ctx,
		j.Client.UpdateItem,
		j.OnRequest,
		&j.truncateReq,
	); err != nil {
		return fmt.Errorf("unable to mark journal record as truncated: %w", err)
	}

	return nil
}

func (j *journ) deleteRecord(ctx context.Context, pos journal.Position) error {
	j.position.Value = marshalPosition(pos)

	if _, err := awsx.Do(
		ctx,
		j.Client.DeleteItem,
		j.OnRequest,
		&j.deleteReq,
	); err != nil {
		return fmt.Errorf("unable to delete journal record: %w", err)
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}

func marshalPosition(pos journal.Position) string {
	return strconv.FormatUint(uint64(pos), 10)
}

func unmarshalPosition(item map[string]types.AttributeValue) (journal.Position, error) {
	attr, err := dynamox.AttrAs[*types.AttributeValueMemberN](item, positionAttr)
	if err != nil {
		return 0, err
	}

	pos, err := strconv.ParseUint(attr.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("item is corrupt: invalid position: %w", err)
	}

	return journal.Position(pos), nil
}

func isTruncated(item map[string]types.AttributeValue) (bool, error) {
	t, ok, err := dynamox.TryAttrAs[*types.AttributeValueMemberBOOL](item, truncatedAttr)
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	if t.Value {
		return true, nil
	}

	return false, errors.New("item is corrupt: truncated attribute is set to false, should be removed")
}
