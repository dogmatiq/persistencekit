package dynamoset

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
)

type setimpl struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	attr struct {
		Set   types.AttributeValueMemberS
		Value types.AttributeValueMemberB
	}

	request struct {
		Has    dynamodb.GetItemInput
		Put    dynamodb.PutItemInput
		Delete dynamodb.DeleteItemInput
	}
}

func (s *setimpl) Name() string {
	return s.attr.Set.Value
}

func (s *setimpl) Has(ctx context.Context, v []byte) (bool, error) {
	s.attr.Value.Value = v

	out, err := awsx.Do(
		ctx,
		s.Client.GetItem,
		s.OnRequest,
		&s.request.Has,
	)
	if err != nil {
		return false, fmt.Errorf("unable to get set value: %w", err)
	}

	return out.Item != nil, nil
}

func (s *setimpl) Add(ctx context.Context, v []byte) error {
	s.request.Put.ConditionExpression = nil
	return s.add(ctx, v)
}

var (
	mustExist    = aws.String("attribute_exists(" + valueAttr + ")")
	mustNotExist = aws.String("attribute_not_exists(" + valueAttr + ")")
)

func (s *setimpl) TryAdd(ctx context.Context, v []byte) (bool, error) {
	s.request.Put.ConditionExpression = mustNotExist
	err := s.add(ctx, v)

	var check *types.ConditionalCheckFailedException
	if errors.As(err, &check) {
		return false, nil
	}

	return true, err
}

func (s *setimpl) Remove(ctx context.Context, v []byte) error {
	s.request.Delete.ConditionExpression = nil
	return s.delete(ctx, v)
}

func (s *setimpl) TryRemove(ctx context.Context, v []byte) (bool, error) {
	s.request.Delete.ConditionExpression = mustExist
	err := s.delete(ctx, v)

	var check *types.ConditionalCheckFailedException
	if errors.As(err, &check) {
		return false, nil
	}

	return true, err
}

func (s *setimpl) add(ctx context.Context, v []byte) error {
	s.attr.Value.Value = v

	if _, err := awsx.Do(
		ctx,
		s.Client.PutItem,
		s.OnRequest,
		&s.request.Put,
	); err != nil {
		return fmt.Errorf("unable to put set value: %w", err)
	}

	return nil
}

func (s *setimpl) delete(ctx context.Context, v []byte) error {
	s.attr.Value.Value = v

	if _, err := awsx.Do(
		ctx,
		s.Client.DeleteItem,
		s.OnRequest,
		&s.request.Delete,
	); err != nil {
		return fmt.Errorf("unable to delete set value: %w", err)
	}

	return nil
}

func (s *setimpl) Close() error {
	return nil
}
