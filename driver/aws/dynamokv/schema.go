package dynamokv

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
)

var (
	// keyspaceAttr is the name of the attribute that stores the keyspace name
	// on each item. Together with [keyAttr], it forms the primary key of the
	// table.
	keyspaceAttr = "S"

	// keyAttr is the name of the attribute that stores the key on each item.
	// Together with [keyspaceAttr], it forms the primary key of the table.
	keyAttr = "K"

	// valueAttr is the name of the attribute that stores the value on each
	// item.
	valueAttr = "V"

	// nonExistentAttr is the name of an attribute that does not exist on any
	// item. It is used to test for the existence of an item without fetching
	// unnecessary data.
	nonExistentAttr = "X"
)

// createTable creates the DynamoDB table if it does not already exist.
func (s *store) createTable(ctx context.Context) error {
	return dynamox.CreateTableIfNotExists(
		ctx,
		s.Client,
		s.Table,
		s.OnRequest,
		dynamox.KeyAttr{
			Name:    &keyspaceAttr,
			Type:    types.ScalarAttributeTypeS,
			KeyType: types.KeyTypeHash,
		},
		dynamox.KeyAttr{
			Name:    &keyAttr,
			Type:    types.ScalarAttributeTypeB,
			KeyType: types.KeyTypeRange,
		},
	)
}

func (ks *keyspace) prepareRequests(table string) {
	key := map[string]types.AttributeValue{
		keyspaceAttr: &ks.attr.Keyspace,
		keyAttr:      &ks.attr.Key,
	}

	// Get fetches the value associated with ks.attr.Key.
	ks.request.Get = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  key,
		ProjectionExpression: aws.String(`#V`),
		ExpressionAttributeNames: map[string]string{
			"#V": valueAttr,
		},
	}

	// Has requests [nonExistentAttr] for the item at ks.attr.Key to check if
	// the item exists at all.
	ks.request.Has = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  key,
		ProjectionExpression: &nonExistentAttr,
	}

	// Range fetches all key/value pairs in the keyspace.
	ks.request.Range = dynamodb.QueryInput{
		TableName:              &table,
		KeyConditionExpression: aws.String(`#S = :S`),
		ProjectionExpression:   aws.String("#K, #V"),
		ExpressionAttributeNames: map[string]string{
			"#S": keyspaceAttr,
			"#K": keyAttr,
			"#V": valueAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":S": &ks.attr.Keyspace,
		},
	}

	// Set sets the value associated with ks.attr.Key to ks.attr.Value.
	ks.request.Set = dynamodb.PutItemInput{
		TableName: &table,
		Item: map[string]types.AttributeValue{
			keyspaceAttr: &ks.attr.Keyspace,
			keyAttr:      &ks.attr.Key,
			valueAttr:    &ks.attr.Value,
		},
	}

	// Delete removes the ks.attr.Key key.
	ks.request.Delete = dynamodb.DeleteItemInput{
		TableName: &table,
		Key:       key,
	}
}
