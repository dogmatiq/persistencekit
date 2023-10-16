package dynamojournal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/journal"
)

// Store is an implementation of [journal.Store] that persists to a DynamoDB
// table.
type Store struct {
	// Client is the DynamoDB client to use.
	Client *dynamodb.Client

	// Table is the table name used for storage of journal records.
	Table string

	// DecorateGetItem is an optional function that is called before each
	// DynamoDB "GetItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateGetItem func(*dynamodb.GetItemInput) []func(*dynamodb.Options)

	// DecorateQuery is an optional function that is called before each DynamoDB
	// "Query" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateQuery func(*dynamodb.QueryInput) []func(*dynamodb.Options)

	// DecoratePutItem is an optional function that is called before each
	// DynamoDB "PutItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecoratePutItem func(*dynamodb.PutItemInput) []func(*dynamodb.Options)

	// DecorateDeleteItem is an optional function that is called before each
	// DynamoDB "DeleteItem" request.
	//
	// It may modify the API input in-place. It returns options that will be
	// applied to the request.
	DecorateDeleteItem func(*dynamodb.DeleteItemInput) []func(*dynamodb.Options)
}

// Open returns the journal with the given name.
func (s *Store) Open(_ context.Context, name string) (journal.Journal, error) {
	j := &journ{
		Client:             s.Client,
		DecorateGetItem:    s.DecorateGetItem,
		DecorateQuery:      s.DecorateQuery,
		DecoratePutItem:    s.DecoratePutItem,
		DecorateDeleteItem: s.DecorateDeleteItem,

		name:     &types.AttributeValueMemberS{Value: name},
		position: &types.AttributeValueMemberN{},
		record:   &types.AttributeValueMemberB{},
	}

	j.boundsQueryRequest = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N`),
		ProjectionExpression:   aws.String("#P"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
		},
		ScanIndexForward: aws.Bool(true),
		Limit:            aws.Int32(1),
	}

	j.getRequest = dynamodb.GetItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
		ProjectionExpression: aws.String(`#R`),
		ExpressionAttributeNames: map[string]string{
			"#R": recordAttr,
		},
	}

	j.rangeQueryRequest = dynamodb.QueryInput{
		TableName:              aws.String(s.Table),
		KeyConditionExpression: aws.String(`#N = :N AND #P >= :P`),
		ProjectionExpression:   aws.String("#P, #R"),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
			"#P": positionAttr,
			"#R": recordAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":N": j.name,
			":P": j.position,
		},
	}

	j.putRequest = dynamodb.PutItemInput{
		TableName:           aws.String(s.Table),
		ConditionExpression: aws.String(`attribute_not_exists(#N)`),
		ExpressionAttributeNames: map[string]string{
			"#N": nameAttr,
		},
		Item: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
			recordAttr:   j.record,
		},
	}

	j.deleteRequest = dynamodb.DeleteItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			nameAttr:     j.name,
			positionAttr: j.position,
		},
	}

	return j, nil
}
