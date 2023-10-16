package dynamojournal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/journal"
)

// journ is an implementation of [journal.Journal] that persists to a DynamoDB
// table.
type journ struct {
	Client             *dynamodb.Client
	DecorateGetItem    func(*dynamodb.GetItemInput) []func(*dynamodb.Options)
	DecorateQuery      func(*dynamodb.QueryInput) []func(*dynamodb.Options)
	DecoratePutItem    func(*dynamodb.PutItemInput) []func(*dynamodb.Options)
	DecorateDeleteItem func(*dynamodb.DeleteItemInput) []func(*dynamodb.Options)

	name     *types.AttributeValueMemberS
	position *types.AttributeValueMemberN
	record   *types.AttributeValueMemberB

	boundsQueryRequest dynamodb.QueryInput
	getRequest         dynamodb.GetItemInput
	rangeQueryRequest  dynamodb.QueryInput
	putRequest         dynamodb.PutItemInput
	deleteRequest      dynamodb.DeleteItemInput
}

func (j *journ) Bounds(ctx context.Context) (begin, end journal.Position, err error) {
	*j.boundsQueryRequest.ScanIndexForward = true
	out, err := awsx.Do(
		ctx,
		j.Client.Query,
		j.DecorateQuery,
		&j.boundsQueryRequest,
	)
	if err != nil || len(out.Items) == 0 {
		return 0, 0, err
	}

	begin, err = parsePosition(out.Items[0])
	if err != nil {
		return 0, 0, err
	}

	*j.boundsQueryRequest.ScanIndexForward = false
	out, err = awsx.Do(
		ctx,
		j.Client.Query,
		j.DecorateQuery,
		&j.boundsQueryRequest,
	)
	if err != nil || len(out.Items) == 0 {
		return 0, 0, err
	}

	end, err = parsePosition(out.Items[0])
	if err != nil {
		return 0, 0, err
	}

	return begin, end + 1, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) ([]byte, error) {
	j.position.Value = formatPosition(pos)

	out, err := awsx.Do(
		ctx,
		j.Client.GetItem,
		j.DecorateGetItem,
		&j.getRequest,
	)
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
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
	fn journal.RangeFunc,
) error {
	j.rangeQueryRequest.ExclusiveStartKey = nil
	j.position.Value = formatPosition(begin)

	expectPos := begin

	for {
		out, err := awsx.Do(
			ctx,
			j.Client.Query,
			j.DecorateQuery,
			&j.rangeQueryRequest,
		)
		if err != nil {
			return err
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
			return nil
		}

		j.rangeQueryRequest.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (j *journ) Append(ctx context.Context, end journal.Position, rec []byte) error {
	j.position.Value = formatPosition(end)
	j.record.Value = rec

	_, err := awsx.Do(
		ctx,
		j.Client.PutItem,
		j.DecoratePutItem,
		&j.putRequest,
	)

	if errors.As(err, new(*types.ConditionalCheckFailedException)) {
		return journal.ErrConflict
	}

	return err
}

func (j *journ) Truncate(ctx context.Context, end journal.Position) error {
	begin, actualEnd, err := j.Bounds(ctx)
	if err != nil {
		return err
	}

	if end >= actualEnd {
		return errors.New("cannot truncate beyond the end of the journal")
	}

	for pos := begin; pos < end; pos++ {
		j.position.Value = formatPosition(pos)

		if _, err := awsx.Do(
			ctx,
			j.Client.DeleteItem,
			j.DecorateDeleteItem,
			&j.deleteRequest,
		); err != nil {
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
