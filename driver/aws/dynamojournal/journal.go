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

var (
	errNoMetaData = errors.New("integrity error: meta-data item does not exist")
)

func provideErrContext(err *error, format string, args ...any) {
	if *err == nil {
		return
	}

	if journal.IsNotFound(*err) {
		return
	}

	if journal.IsConflict(*err) {
		return
	}

	*err = fmt.Errorf(format+": %w", append(args, *err)...)
}

// journ is an implementation of [journal.BinaryJournal] that persists to a
// DynamoDB table.
type journ struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	attr struct {
		Journal        types.AttributeValueMemberS // [journalAttr]
		Pos            types.AttributeValueMemberN // [positionAttr]
		Record         types.AttributeValueMemberB // [recordAttr]
		BeginPos       types.AttributeValueMemberN // [metaDataBeginPositionAttr]
		UncompactedPos types.AttributeValueMemberN // [metaDataUncompactedPositionAttr]
	}

	request struct {
		SetBeginPos       dynamodb.UpdateItemInput
		SetUncompactedPos dynamodb.UpdateItemInput
		LoadBegin         dynamodb.GetItemInput
		LoadEnd           dynamodb.QueryInput
		Get               dynamodb.GetItemInput
		Range             dynamodb.QueryInput
		Append            dynamodb.PutItemInput
		Compact           dynamodb.UpdateItemInput
	}
}

// init initializes the journal meta-data and compacts any records that have
// been truncated but not yet compacted.
func (j *journ) init(ctx context.Context, table, name string) (err error) {
	defer provideErrContext(&err, "unable to initialize the %q journal", name)

	j.attr.Journal.Value = name
	j.prepareRequests(table)

	uncompacted, err := j.initMetaData(ctx, table)
	if err != nil {
		return err
	}

	return j.compact(ctx, uncompacted)
}

// initMetaData initializes the meta-data item for the journal.
//
// It returns the interval of records that have been truncated but not yet
// compacted.
func (j *journ) initMetaData(ctx context.Context, table string) (journal.Interval, error) {
	j.attr.BeginPos.Value = marshalPosition(0)
	j.attr.UncompactedPos.Value = marshalPosition(0)

	_, err := awsx.Do(
		ctx,
		j.Client.PutItem,
		j.OnRequest,
		&dynamodb.PutItemInput{
			TableName: &table,
			Item: map[string]types.AttributeValue{
				journalAttr:                     &j.attr.Journal,
				positionAttr:                    &metaDataPosition,
				metaDataBeginPositionAttr:       &j.attr.BeginPos,
				metaDataUncompactedPositionAttr: &j.attr.UncompactedPos,
			},
			ExpressionAttributeNames: map[string]string{
				"#J": journalAttr,
			},
			ConditionExpression:                 aws.String(`attribute_not_exists(#J)`),
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		},
	)

	var conflict *types.ConditionalCheckFailedException
	if errors.As(err, &conflict) {
		return unmarshalUncompactedInterval(conflict.Item)
	}

	return journal.Interval{}, err
}

func (j *journ) Name() string {
	return j.attr.Journal.Value
}

func (j *journ) Bounds(ctx context.Context) (journal.Interval, error) {
	end, empty, err := j.loadEnd(ctx)
	if err != nil {
		return journal.Interval{}, err
	}

	if empty {
		return journal.Interval{
			Begin: end,
			End:   end,
		}, nil
	}

	begin, err := j.loadBegin(ctx)
	if err != nil {
		return journal.Interval{}, err
	}

	return journal.Interval{
		Begin: begin,
		End:   end,
	}, nil
}

func (j *journ) loadBegin(ctx context.Context) (_ journal.Position, err error) {
	defer provideErrContext(&err, "unable to load lower bound of the %q journal", j.Name())

	out, err := awsx.Do(
		ctx,
		j.Client.GetItem,
		j.OnRequest,
		&j.request.LoadBegin,
	)
	if err != nil {
		return 0, err
	}

	if out.Item == nil {
		return 0, errNoMetaData
	}

	return unmarshalPosition(out.Item, metaDataBeginPositionAttr)
}

func (j *journ) loadEnd(ctx context.Context) (end journal.Position, empty bool, err error) {
	defer provideErrContext(&err, "unable to load upper bound of the %q journal", j.Name())

	ok, err := dynamox.QueryOne(
		ctx,
		j.Client,
		j.OnRequest,
		&j.request.LoadEnd,
		func(ctx context.Context, item map[string]types.AttributeValue) error {
			var err error

			empty, err := isMetaData(item)
			if empty || err != nil {
				return err
			}

			pos, err := unmarshalPosition(item, positionAttr)
			if err != nil {
				return err
			}

			// The [begin, end) range is half-open, so the end position is the
			// one AFTER the most recent record.
			end = pos + 1

			// If the most recent record has been compacted (and therefore)
			// truncated, the journal is empty with bounds of [end, end).
			empty, err = isCompacted(item)

			return err
		},
	)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, errNoMetaData
	}

	return end, empty, nil
}

func (j *journ) Get(ctx context.Context, pos journal.Position) (_ []byte, err error) {
	defer provideErrContext(&err, "unable to get record at position %d of the %q journal", pos, j.Name())

	j.attr.Pos.Value = marshalPosition(pos)

	out, err := awsx.Do(
		ctx,
		j.Client.GetItem,
		j.OnRequest,
		&j.request.Get,
	)
	if err != nil {
		return nil, err
	}

	if out.Item == nil {
		return nil, journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	isTrunc, err := isCompacted(out.Item)
	if err != nil {
		return nil, err
	}

	if isTrunc {
		return nil, journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	rec, err := dynamox.AsBytes(out.Item, recordAttr)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func (j *journ) Range(
	ctx context.Context,
	pos journal.Position,
	fn journal.BinaryRangeFunc,
) (err error) {
	defer provideErrContext(&err, "unable to range over records starting at position %d of the %q journal", pos, j.Name())

	j.attr.Pos.Value = marshalPositionBefore(pos)
	expectPos := pos

	if err := dynamox.QueryRange(
		ctx,
		j.Client,
		j.OnRequest,
		&j.request.Range,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			pos, err := unmarshalPosition(item, positionAttr)
			if err != nil {
				return false, err
			}

			if pos != expectPos {
				return false, journal.RecordNotFoundError{
					Journal:  j.Name(),
					Position: expectPos,
				}
			}

			expectPos++

			rec, err := dynamox.AsBytes(item, recordAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, pos, rec)
		},
	); err != nil {
		return err
	}

	if expectPos == pos {
		return journal.RecordNotFoundError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	return nil
}

func (j *journ) Append(ctx context.Context, pos journal.Position, rec []byte) (err error) {
	defer provideErrContext(&err, "unable to append record at position %d of the %q journal", pos, j.Name())

	j.attr.Pos.Value = marshalPosition(pos)
	j.attr.Record.Value = rec

	_, err = awsx.Do(
		ctx,
		j.Client.PutItem,
		j.OnRequest,
		&j.request.Append,
	)

	var conflict *types.ConditionalCheckFailedException
	if errors.As(err, &conflict) {
		return journal.ConflictError{
			Journal:  j.Name(),
			Position: pos,
		}
	}

	return err
}

func (j *journ) Truncate(ctx context.Context, pos journal.Position) (err error) {
	defer provideErrContext(&err, "unable to truncate records before position %d of the %q journal", pos, j.Name())

	j.attr.BeginPos.Value = marshalPosition(pos)

	res, err := awsx.Do(
		ctx,
		j.Client.UpdateItem,
		j.OnRequest,
		&j.request.SetBeginPos,
	)
	if err != nil {
		if errors.As(err, new(*types.ConditionalCheckFailedException)) {
			return nil
		}
		return err
	}

	uncompacted, err := unmarshalUncompactedInterval(res.Attributes)
	if err != nil {
		return err
	}

	return j.compact(ctx, uncompacted)
}

func (j *journ) compact(ctx context.Context, uncompacted journal.Interval) (err error) {
	defer provideErrContext(&err, "unable to compact records within %s of the %q journal", uncompacted, j.Name())

	if uncompacted.IsEmpty() {
		return nil
	}

	for _, pos := range uncompacted.Positions() {
		j.attr.Pos.Value = marshalPosition(pos)

		if _, err := awsx.Do(
			ctx,
			j.Client.UpdateItem,
			j.OnRequest,
			&j.request.Compact,
		); err != nil {
			return fmt.Errorf("unable to compact record at position %d: %w", pos, err)
		}
	}

	j.attr.UncompactedPos.Value = marshalPosition(uncompacted.End)

	if _, err = awsx.Do(
		ctx,
		j.Client.UpdateItem,
		j.OnRequest,
		&j.request.SetUncompactedPos,
	); err != nil {
		return fmt.Errorf("unable to update uncompacted position: %w", err)
	}

	return nil
}

func (j *journ) Close() error {
	return nil
}

// isMetaData returns true if the item is the meta-data item.
func isMetaData(item map[string]types.AttributeValue) (bool, error) {
	pos, err := dynamox.AsNumericString(item, positionAttr)
	return pos == "-1", err
}

// isCompacted returns true if the item is a compacted record.
func isCompacted(item map[string]types.AttributeValue) (bool, error) {
	return dynamox.AsBool(item, recordIsCompactedAttr)
}

// marshalPosition returns the string representation of pos.
func marshalPosition(pos journal.Position) string {
	return strconv.FormatUint(uint64(pos), 10)
}

// marshalPositionBefore returns the string representation of pos - 1.
func marshalPositionBefore(pos journal.Position) string {
	if pos == 0 {
		return "-1"
	}
	return strconv.FormatUint(uint64(pos)-1, 10)
}

// unmarshalPosition returns the journal position represented by a number
// attribute with the given key.
func unmarshalPosition(item map[string]types.AttributeValue, key string) (journal.Position, error) {
	return dynamox.AsUint[journal.Position](item, key)
}

// unmarshalUncompactedInterval returns the interval of records that have been
// truncated but not yet compacted from the meta-data item.
func unmarshalUncompactedInterval(item map[string]types.AttributeValue) (journal.Interval, error) {
	begin, err := unmarshalPosition(item, metaDataBeginPositionAttr)
	if err != nil {
		return journal.Interval{}, err
	}

	uncompacted, err := unmarshalPosition(item, metaDataUncompactedPositionAttr)
	if err != nil {
		return journal.Interval{}, err
	}

	return journal.Interval{
		Begin: uncompacted,
		End:   begin,
	}, nil
}
