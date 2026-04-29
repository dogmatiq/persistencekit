package dynamoset

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/set"
)

type setimpl struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	attr struct {
		Set    types.AttributeValueMemberS
		Member types.AttributeValueMemberB
	}

	request struct {
		Has    dynamodb.GetItemInput
		Range  dynamodb.QueryInput
		Put    dynamodb.PutItemInput
		Delete dynamodb.DeleteItemInput
	}
}

func (s *setimpl) Name() string {
	return s.attr.Set.Value
}

func (s *setimpl) Has(ctx context.Context, v []byte) (bool, error) {
	s.attr.Member.Value = v

	out, err := awsx.Do(
		ctx,
		s.Client.GetItem,
		s.OnRequest,
		&s.request.Has,
	)
	if err != nil {
		return false, fmt.Errorf("unable to get set member: %w", err)
	}

	return out.Item != nil, nil
}

func (s *setimpl) Add(ctx context.Context, v []byte) error {
	s.request.Put.ConditionExpression = nil
	return s.add(ctx, v)
}

var (
	mustExist    = aws.String("attribute_exists(" + memberAttr + ")")
	mustNotExist = aws.String("attribute_not_exists(" + memberAttr + ")")
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

func (s *setimpl) Range(ctx context.Context, fn set.BinaryRangeFunc) error {
	if err := dynamox.QueryRange(
		ctx,
		s.Client,
		s.OnRequest,
		&s.request.Range,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			value, err := dynamox.AsBytes(item, memberAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, value)
		},
	); err != nil {
		return fmt.Errorf("unable to range over set: %w", err)
	}

	return nil
}

func (s *setimpl) add(ctx context.Context, v []byte) error {
	s.attr.Member.Value = v

	if _, err := awsx.Do(
		ctx,
		s.Client.PutItem,
		s.OnRequest,
		&s.request.Put,
	); err != nil {
		return fmt.Errorf("unable to put set member: %w", err)
	}

	return nil
}

func (s *setimpl) delete(ctx context.Context, v []byte) error {
	s.attr.Member.Value = v

	if _, err := awsx.Do(
		ctx,
		s.Client.DeleteItem,
		s.OnRequest,
		&s.request.Delete,
	); err != nil {
		return fmt.Errorf("unable to delete set member: %w", err)
	}

	return nil
}

func (s *setimpl) Close() error {
	return nil
}
