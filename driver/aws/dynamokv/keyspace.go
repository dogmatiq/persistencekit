package dynamokv

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/dynamox"
	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	attr struct {
		Keyspace types.AttributeValueMemberS
		Key      types.AttributeValueMemberB
		Value    types.AttributeValueMemberB
		Revision types.AttributeValueMemberN
	}

	request struct {
		Get                 dynamodb.GetItemInput
		Has                 dynamodb.GetItemInput
		Range               dynamodb.QueryInput
		Update              dynamodb.UpdateItemInput
		UpdateUnconditional dynamodb.UpdateItemInput
		Delete              dynamodb.DeleteItemInput
		DeleteUnconditional dynamodb.DeleteItemInput
	}
}

func (ks *keyspace) Name() string {
	return ks.attr.Keyspace.Value
}

func (ks *keyspace) Get(ctx context.Context, k []byte) ([]byte, uint64, error) {
	ks.attr.Key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.OnRequest,
		&ks.request.Get,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get keyspace pair: %w", err)
	}
	if out.Item == nil {
		return nil, 0, err
	}

	v, err := dynamox.AsBytes(out.Item, valueAttr)
	if err != nil {
		return nil, 0, err
	}

	r, err := dynamox.AsUint[uint64](out.Item, revisionAttr)
	if err != nil {
		return nil, 0, err
	}

	return v, r, nil
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (bool, error) {
	ks.attr.Key.Value = k

	out, err := awsx.Do(
		ctx,
		ks.Client.GetItem,
		ks.OnRequest,
		&ks.request.Has,
	)
	if err != nil {
		return false, fmt.Errorf("unable to get keyspace pair: %w", err)
	}

	return out.Item != nil, nil
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte, r uint64) error {
	ks.attr.Key.Value = k
	ks.attr.Value.Value = v
	ks.attr.Revision.Value = strconv.FormatUint(r, 10)

	convertConflictError := func(message string, err error) error {
		var conflict *types.ConditionalCheckFailedException
		if errors.As(err, &conflict) {
			return kv.ConflictError[[]byte]{
				Keyspace: ks.attr.Keyspace.Value,
				Key:      k,
				Revision: r,
			}
		}

		return fmt.Errorf("%s: %w", message, err)
	}

	if len(v) == 0 {
		if _, err := awsx.Do(
			ctx,
			ks.Client.DeleteItem,
			ks.OnRequest,
			&ks.request.Delete,
		); err != nil {
			return convertConflictError("unable to delete keyspace pair", err)
		}

		return nil
	}

	if _, err := awsx.Do(
		ctx,
		ks.Client.UpdateItem,
		ks.OnRequest,
		&ks.request.Update,
	); err != nil {
		return convertConflictError("unable to update keyspace pair", err)
	}

	return nil
}

func (ks *keyspace) SetUnconditional(ctx context.Context, k, v []byte) error {
	ks.attr.Key.Value = k
	ks.attr.Value.Value = v

	if len(v) == 0 {
		if _, err := awsx.Do(
			ctx,
			ks.Client.DeleteItem,
			ks.OnRequest,
			&ks.request.DeleteUnconditional,
		); err != nil {
			return fmt.Errorf("unable to delete keyspace pair: %w", err)
		}

		return nil
	}

	if _, err := awsx.Do(
		ctx,
		ks.Client.UpdateItem,
		ks.OnRequest,
		&ks.request.UpdateUnconditional,
	); err != nil {
		return fmt.Errorf("unable to update keyspace pair: %w", err)
	}

	return nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) error {
	if err := dynamox.QueryRange(
		ctx,
		ks.Client,
		ks.OnRequest,
		&ks.request.Range,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			k, err := dynamox.AsBytes(item, keyAttr)
			if err != nil {
				return false, err
			}

			v, err := dynamox.AsBytes(item, valueAttr)
			if err != nil {
				return false, err
			}

			r, err := dynamox.AsUint[uint64](item, revisionAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, k, v, r)
		},
	); err != nil {
		return fmt.Errorf("unable to range over keyspace: %w", err)
	}

	return nil
}

func (ks *keyspace) Close() error {
	return nil
}
