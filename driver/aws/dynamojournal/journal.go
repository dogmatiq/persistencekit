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

	boundsQueryRequest dynamodb.QueryInput
	getRequest         dynamodb.GetItemInput
	rangeQueryRequest  dynamodb.QueryInput
	putRequest         dynamodb.PutItemInput
	truncateRequest    dynamodb.UpdateItemInput
	deleteRequest      dynamodb.DeleteItemInput
}

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	for {
		// First we look for the most recent record by scanning the table
		// backwards.
		*j.boundsQueryRequest.ScanIndexForward = false

		// We want to include all records, including those that have been
		// truncated so that we can detect if the journal is empty.
		j.boundsQueryRequest.FilterExpression = nil

		out, err := awsx.Do(
			ctx,
			j.Client.Query,
			j.OnRequest,
			&j.boundsQueryRequest,
		)
		if err != nil {
			return 0, 0, fmt.Errorf("unable to query last journal record: %w", err)
		}

		// There are no records at all, so the journal is truly empty.
		if len(out.Items) == 0 {
			return 0, 0, nil
		}

		end, err = parsePosition(out.Items[0])
		if err != nil {
			return 0, 0, err
		}

		// The [begin, end) range is half-open, so the end position is the one
		// AFTER the most recent record.
		end++

		truncated, err := isTruncated(out.Items[0])
		if err != nil {
			return 0, 0, err
		}

		// If the most recent record has been truncated, the journal is
		// effectively empty with bounds of [end, end).
		if truncated {
			return end, end, nil
		}

		// We know there is at least one non-truncated record, so now we search
		// for the oldest non-truncated record to find the lower bound.
		*j.boundsQueryRequest.ScanIndexForward = true
		j.boundsQueryRequest.FilterExpression = aws.String(`attribute_not_exists(#T)`)

		out, err = awsx.Do(
			ctx,
			j.Client.Query,
			j.OnRequest,
			&j.boundsQueryRequest,
		)
		if err != nil {
			return 0, 0, fmt.Errorf("unable to query first journal record: %w", err)
		}

		// We were expecting to find a record, because we know that the most
		// recent record exists and is not truncated. If we find nothing it
		// likely means that the journal has been truncated at some point in
		// between the two queries we've made. We'll retry the whole process to
		// ensure we get a consistent result.
		if len(out.Items) == 0 {
			continue
		}

		begin, err = parsePosition(out.Items[0])
		if err != nil {
			return 0, 0, err
		}

		return begin, end, nil
	}
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	j.position.Value = formatPosition(pos)

	out, err := awsx.Do(
		ctx,
		j.Client.GetItem,
		j.OnRequest,
		&j.getRequest,
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
	begin journal.Position,
	fn journal.BinaryRangeFunc,
) error {
	j.rangeQueryRequest.ExclusiveStartKey = nil
	j.position.Value = formatPosition(begin)

	expectPos := begin

	for {
		out, err := awsx.Do(
			ctx,
			j.Client.Query,
			j.OnRequest,
			&j.rangeQueryRequest,
		)
		if err != nil {
			return fmt.Errorf("unable to query journal records: %w", err)
		}

		for _, item := range out.Items {
			pos, err := parsePosition(item)
			if err != nil {
				return err
			}

			if pos != expectPos {
				return journal.ErrNotFound
			}

			expectPos++

			rec, err := dynamox.AttrAs[*types.AttributeValueMemberB](item, recordAttr)
			if err != nil {
				return err
			}

			ok, err := fn(ctx, pos, rec.Value)
			if !ok || err != nil {
				return err
			}
		}

		if out.LastEvaluatedKey == nil {
			if expectPos == begin {
				return journal.ErrNotFound
			}
			return nil
		}

		j.rangeQueryRequest.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	j.position.Value = formatPosition(end)
	j.record.Value = rec

	if _, err := awsx.Do(
		ctx,
		j.Client.PutItem,
		j.OnRequest,
		&j.putRequest,
	); err != nil {
		if errors.As(err, new(*types.ConditionalCheckFailedException)) {
			return journal.ErrConflict
		}
		return fmt.Errorf("unable to put journal record: %w", err)
	}

	return nil
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
	begin, actualEnd, err := j.Bounds(ctx)
	if err != nil {
		return err
	}

	if end > actualEnd {
		return errors.New("cannot truncate beyond the end of the journal")
	}

	for pos := begin; pos < end; pos++ {
		j.position.Value = formatPosition(pos)

		var err error
		if pos+1 == actualEnd {
			_, err = awsx.Do(
				ctx,
				j.Client.UpdateItem,
				j.OnRequest,
				&j.truncateRequest,
			)
		} else {
			_, err = awsx.Do(
				ctx,
				j.Client.DeleteItem,
				j.OnRequest,
				&j.deleteRequest,
			)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}

// parsePosition parses the position attribute in the given item.
func parsePosition(item map[string]types.AttributeValue) (journal.Position, error) {
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

func formatPosition(pos journal.Position) string {
	return strconv.FormatUint(uint64(pos), 10)
}

func isTruncated(item map[string]types.AttributeValue) (bool, error) {
	t, ok, err := dynamox.TryAttrAs[*types.AttributeValueMemberBOOL](item, truncatedAttr)
	return ok && t.Value, err
}
