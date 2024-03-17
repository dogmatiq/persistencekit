package dynamojournal

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

const (
	nameAttr     = "Name"
	positionAttr = "Position"
	recordAttr   = "Record"
)

// CreateTable creates a DynamoDB table for use with a [BinaryStore].
func CreateTable(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	decorators ...func(*dynamodb.CreateTableInput) []func(*dynamodb.Options),
) error {
	_, err := awsx.Do(
		ctx,
		client.CreateTable,
		func(in *dynamodb.CreateTableInput) []func(*dynamodb.Options) {
			var options []func(*dynamodb.Options)
			for _, dec := range decorators {
				options = append(options, dec(in)...)
			}

			return options
		},
		&dynamodb.CreateTableInput{
			TableName: aws.String(table),
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String(nameAttr),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String(positionAttr),
					AttributeType: types.ScalarAttributeTypeN,
				},
			},
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(nameAttr),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String(positionAttr),
					KeyType:       types.KeyTypeRange,
				},
			},
			BillingMode: types.BillingModePayPerRequest,
		},
	)

	if errors.As(err, new(*types.ResourceInUseException)) {
		return nil
	}

	return err
}
