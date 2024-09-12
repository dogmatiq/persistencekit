package dynamojournal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
)

var (
	// journalAttr is the name of the attribute that stores the journal name on
	// each item. Together with [positionAttr], it forms the primary key of the
	// table.
	journalAttr = "J"

	// positionAttr is the name of the attribute that stores the position on
	// each item that represents a journal record. Together with [journalAttr],
	// it forms the primary key of the table.
	//
	// As a special case, the "meta-data" item has a position of -1.
	positionAttr = "P"

	// recordAttr is the name of the attribute that stores the record data
	// itself. It is not present on the "meta-data" item.
	recordAttr = "R"

	// recordIsCompactedAttr is the name of the attribute that stores a boolean
	// flag indicating whether a truncated record has been compacted.
	//
	// This flag is only added when a truncated record is "compacted". The
	// authoritative source of whether a record has been truncated is the
	// [metaDataBeginPositionAttr] attribute in the "meta-data" item.
	recordIsCompactedAttr = "C"

	// metaDataBeginPositionAttr is the name of the attribute that stores the
	// position of the first record in the journal. It defaults to zero and is
	// updated when records are truncated.
	metaDataBeginPositionAttr = "B"

	// metaDataUncompactedPositionAttr is the name of the attribute that stores
	// the position of the next uncompacted record in the journal.
	//
	// This attribute is only updated after a batch of records have been
	// compacted, and therefore serves as a hint as to where compaction should
	// begin, but is not authoritative.
	metaDataUncompactedPositionAttr = "U"

	// metaDataPosition is the value of the [positionAttr] attribute for the
	// "meta-data" item.
	metaDataPosition = types.AttributeValueMemberN{
		Value: marshalPositionBefore(0),
	}
)

// createTable creates the DynamoDB table if it does not already exist.
func (s *store) createTable(ctx context.Context) error {
	return dynamox.CreateTableIfNotExists(
		ctx,
		s.Client,
		s.Table,
		s.OnRequest,
		dynamox.KeyAttr{
			Name:    &journalAttr,
			Type:    types.ScalarAttributeTypeS,
			KeyType: types.KeyTypeHash,
		},
		dynamox.KeyAttr{
			Name:    &positionAttr,
			Type:    types.ScalarAttributeTypeN,
			KeyType: types.KeyTypeRange,
		},
	)
}

// prepareRequests prepares the DynamoDB API requests used by the journal.
//
// The requests are built once and reused for the lifetime of the journal to
// avoid repeated heap allocations of the same data.
func (j *journ) prepareRequests(table string) {
	metaDataKey := map[string]types.AttributeValue{
		journalAttr:  &j.attr.Journal,
		positionAttr: &metaDataPosition,
	}

	recordAtPositionKey := map[string]types.AttributeValue{
		journalAttr:  &j.attr.Journal,
		positionAttr: &j.attr.Pos,
	}

	// UpdateBegin updates the [metaDataBeginPositionAttr] attribute on the
	// "meta-data" item to j.attr.Begin.
	j.request.SetBeginPos = dynamodb.UpdateItemInput{
		TableName:        &table,
		Key:              metaDataKey,
		UpdateExpression: aws.String(`SET #B = :B`),
		ExpressionAttributeNames: map[string]string{
			"#B": metaDataBeginPositionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":B": &j.attr.BeginPos,
		},
		ConditionExpression: aws.String(`#B < :B`),
		ReturnValues:        types.ReturnValueAllNew,
	}

	// UpdateUncompacted updates the [metaDataUncompactedPositionAttr] attribute
	// on the "meta-data" item to j.attr.Uncompacted.
	j.request.SetUncompactedPos = dynamodb.UpdateItemInput{
		TableName:        &table,
		Key:              metaDataKey,
		UpdateExpression: aws.String(`SET #U = :U`),
		ExpressionAttributeNames: map[string]string{
			"#U": metaDataUncompactedPositionAttr,
			"#B": metaDataBeginPositionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":U": &j.attr.UncompactedPos,
		},
		ConditionExpression: aws.String(`#U < :U AND #B >= :U`),
	}

	// LoadBegin fetches the [metaDataBeginPositionAttr] attribute from the
	// "meta-data" item.
	j.request.LoadBegin = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  metaDataKey,
		ProjectionExpression: aws.String(`#B`),
		ExpressionAttributeNames: map[string]string{
			"#B": metaDataBeginPositionAttr,
		},
	}

	// LoadEnd loads the "end" position by querying the journal in reverse order
	// to find the item with the highest [positionAttr].
	j.request.LoadEnd = dynamodb.QueryInput{
		TableName:              &table,
		KeyConditionExpression: aws.String(`#J = :J`),
		ProjectionExpression:   aws.String("#P, #C"),
		ExpressionAttributeNames: map[string]string{
			"#J": journalAttr,
			"#P": positionAttr,
			"#C": recordIsCompactedAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":J": &j.attr.Journal,
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	}

	// Get fetches the record at j.attr.Position.
	j.request.Get = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  recordAtPositionKey,
		ProjectionExpression: aws.String(`#C, #R`),
		ExpressionAttributeNames: map[string]string{
			"#C": recordIsCompactedAttr,
			"#R": recordAttr,
		},
	}

	// Range fetches all non-truncated records in the journal starting at
	// j.attr.Position.
	j.request.Range = dynamodb.QueryInput{
		TableName:              &table,
		KeyConditionExpression: aws.String(`#J = :J`),
		FilterExpression:       aws.String(`attribute_not_exists(#C)`),
		ProjectionExpression:   aws.String("#P, #R"),
		ExpressionAttributeNames: map[string]string{
			"#J": journalAttr,
			"#P": positionAttr,
			"#C": recordIsCompactedAttr,
			"#R": recordAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":J": &j.attr.Journal,
		},
		ExclusiveStartKey: recordAtPositionKey,
	}

	// appendReq adds a new record to the journal at j.attr.Position.
	j.request.Append = dynamodb.PutItemInput{
		TableName: &table,
		ExpressionAttributeNames: map[string]string{
			"#J": journalAttr,
		},
		Item: map[string]types.AttributeValue{
			journalAttr:  &j.attr.Journal,
			positionAttr: &j.attr.Pos,
			recordAttr:   &j.attr.Record,
		},

		// Fail if the record exists so we can return [journal.ErrConflict].
		ConditionExpression: aws.String(`attribute_not_exists(#J)`),
	}

	// Compact compacts the truncated record at j.attr.Position by setting the
	// [recordTruncatedAttr] flag and removing the [recordAttr].
	j.request.Compact = dynamodb.UpdateItemInput{
		TableName:        &table,
		Key:              recordAtPositionKey,
		UpdateExpression: aws.String(`SET #C = :C REMOVE #R`),
		ExpressionAttributeNames: map[string]string{
			"#C": recordIsCompactedAttr,
			"#R": recordAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":C": dynamox.True,
		},
	}
}
