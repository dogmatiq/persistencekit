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

	// revisionAttr is the name of the attribute that stores the revision of
	// each item.
	revisionAttr = "R"

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

	zero := &types.AttributeValueMemberN{Value: "0"}
	one := &types.AttributeValueMemberN{Value: "1"}

	// Get fetches the value associated with ks.attr.Key.
	ks.request.Get = dynamodb.GetItemInput{
		TableName:            &table,
		Key:                  key,
		ProjectionExpression: aws.String(`#V, #R`),
		ExpressionAttributeNames: map[string]string{
			"#V": valueAttr,
			"#R": revisionAttr,
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
		ProjectionExpression:   aws.String(`#K, #V, #R`),
		ExpressionAttributeNames: map[string]string{
			"#S": keyspaceAttr,
			"#K": keyAttr,
			"#V": valueAttr,
			"#R": revisionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":S": &ks.attr.Keyspace,
		},
	}

	// Update sets the value associated with ks.attr.Key to ks.attr.Value at
	// revision ks.attr.CurrentRevision.
	ks.request.Update = dynamodb.UpdateItemInput{
		TableName: &table,
		Key:       key,
		ExpressionAttributeNames: map[string]string{
			"#V": valueAttr,
			"#R": revisionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":V": &ks.attr.Value,
			":R": &ks.attr.Revision,
			":0": zero,
			":1": one,
		},
		UpdateExpression: aws.String(`SET #V = :V ADD #R :1`),

		// Fail if the revision does not match so we can return
		// [kv.ConflictError].
		ConditionExpression: aws.String(`(attribute_not_exists(#R) AND :R = :0) OR #R = :R`),
	}

	// SetUnconditional sets the value associated with ks.attr.Key to
	// ks.attr.Value at any revision.
	ks.request.UpdateUnconditional = dynamodb.UpdateItemInput{
		TableName: &table,
		Key:       key,
		ExpressionAttributeNames: map[string]string{
			"#V": valueAttr,
			"#R": revisionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":V": &ks.attr.Value,
			":1": one,
		},
		UpdateExpression: aws.String(`SET #V = :V ADD #R :1`),
	}

	// Delete removes the ks.attr.Key key at revision ks.attr.CurrentRevision.
	ks.request.Delete = dynamodb.DeleteItemInput{
		TableName: &table,
		Key:       key,
		ExpressionAttributeNames: map[string]string{
			"#R": revisionAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":R": &ks.attr.Revision,
			":0": zero,
		},

		// Fail if the revision does not match so we can return
		// [kv.ConflictError].
		ConditionExpression: aws.String(`(attribute_not_exists(#R) AND :R = :0) OR #R = :R`),
	}

	// DeleteUnconditional removes the ks.attr.Key key unconditionally.
	ks.request.DeleteUnconditional = dynamodb.DeleteItemInput{
		TableName: &table,
		Key:       key,
	}
}
