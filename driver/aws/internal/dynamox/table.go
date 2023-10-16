package dynamox

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sso/types"
)

// DeleteTableIfNotExists deletes a DynamoDB table if it does not exist.
func DeleteTableIfNotExists(
	ctx context.Context,
	client *dynamodb.Client,
	table string,
) error {
	if _, err := client.DeleteTable(
		ctx,
		&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		},
	); err != nil {
		if !errors.As(err, new(*types.ResourceNotFoundException)) {
			return err
		}
	}

	return nil
}
