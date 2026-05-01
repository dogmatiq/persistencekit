package xdynamodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xaws"
)

// KeyAttr describes a key attribute of a DynamoDB table.
type KeyAttr struct {
	Name    *string
	Type    types.ScalarAttributeType
	KeyType types.KeyType
}

// CreateTableIfNotExists creates a DynamoDB table if it does not exist. It
// returns true if the table was created, or false if it already existed.
func CreateTableIfNotExists(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
	key ...KeyAttr,
) (bool, error) {
	var created bool

	res, err := xaws.Do(
		ctx,
		client.DescribeTable,
		onRequest,
		&dynamodb.DescribeTableInput{
			TableName: &table,
		},
	)
	if errors.As(err, new(*types.ResourceNotFoundException)) {
		var err error
		created, err = createTable(ctx, client, table, onRequest, key)
		if err != nil {
			return false, err
		}
	} else if err != nil {
		return false, fmt.Errorf("unable to describe DynamoDB table: %w", err)
	} else if res.Table.TableStatus == types.TableStatusActive {
		return false, nil
	}

	if err := waitForTable(ctx, client, table, onRequest); err != nil {
		return false, err
	}

	return created, nil
}

// createTable issues a CreateTable request. It returns true if the table was
// created, or false if it already existed due to a concurrent creation.
func createTable(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
	key []KeyAttr,
) (bool, error) {
	req := &dynamodb.CreateTableInput{
		TableName:   &table,
		BillingMode: types.BillingModePayPerRequest,
	}

	for _, k := range key {
		req.AttributeDefinitions = append(
			req.AttributeDefinitions,
			types.AttributeDefinition{
				AttributeName: k.Name,
				AttributeType: k.Type,
			},
		)

		req.KeySchema = append(
			req.KeySchema,
			types.KeySchemaElement{
				AttributeName: k.Name,
				KeyType:       k.KeyType,
			},
		)
	}

	if _, err := xaws.Do(
		ctx,
		client.CreateTable,
		onRequest,
		req,
	); err != nil {
		if errors.As(err, new(*types.ResourceInUseException)) {
			return false, nil
		}
		return false, fmt.Errorf("unable to create DynamoDB table: %w", err)
	}

	return true, nil
}

// waitForTable blocks until the given DynamoDB table is created.
func waitForTable(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
) error {
	in := &dynamodb.DescribeTableInput{
		TableName: &table,
	}

	var options []func(*dynamodb.Options)
	if onRequest != nil {
		options = onRequest(in)
	}

	w := dynamodb.NewTableExistsWaiter(
		client,
		func(opts *dynamodb.TableExistsWaiterOptions) {
			opts.ClientOptions = options
		},
	)

	// We set the maximum wait time quite high, as the deadline from ctx, if
	// shorter, will take precedence.
	return w.Wait(ctx, in, 1*time.Minute)
}

// DeleteTableIfExists deletes a DynamoDB table if it exists.
func DeleteTableIfExists(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
	onRequest func(any) []func(*dynamodb.Options),
) error {
	if _, err := xaws.Do(
		ctx,
		client.DeleteTable,
		onRequest,
		&dynamodb.DeleteTableInput{
			TableName: &table,
		},
	); err != nil {
		if errors.As(err, new(*types.ResourceNotFoundException)) {
			return nil
		}

		return fmt.Errorf("unable to delete DynamoDB table: %w", err)
	}

	return nil
}
