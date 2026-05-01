package dynamokv

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xaws"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/x/xdynamodb"
	"github.com/dogmatiq/persistencekit/internal/kvrevision"
	"github.com/dogmatiq/persistencekit/kv"
)

type keyspace struct {
	Client    *dynamodb.Client
	OnRequest func(any) []func(*dynamodb.Options)

	attr struct {
		Keyspace   types.AttributeValueMemberS
		Key        types.AttributeValueMemberB
		Value      types.AttributeValueMemberB
		Generation types.AttributeValueMemberN
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

func (ks *keyspace) Get(ctx context.Context, k []byte) ([]byte, kv.Revision, error) {
	ks.attr.Key.Value = k

	out, err := xaws.Do(
		ctx,
		ks.Client.GetItem,
		ks.OnRequest,
		&ks.request.Get,
	)
	if err != nil {
		return nil, "", fmt.Errorf("unable to get keyspace pair: %w", err)
	}
	if out.Item == nil {
		return nil, "", err
	}

	v, err := xdynamodb.AsBytes(out.Item, valueAttr)
	if err != nil {
		return nil, "", err
	}

	gen, err := xdynamodb.AsUint[uint64](out.Item, generationAttr)
	if err != nil {
		return nil, "", err
	}

	return v, kvrevision.MarshalGeneration(gen), nil
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (bool, error) {
	ks.attr.Key.Value = k

	out, err := xaws.Do(
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

func (ks *keyspace) Set(ctx context.Context, k, v []byte, r kv.Revision) (kv.Revision, error) {
	ks.attr.Key.Value = k
	ks.attr.Value.Value = v
	gen, ok := kvrevision.TryUnmarshalGeneration(r)
	if !ok {
		return "", kv.ConflictError[[]byte]{
			Keyspace: ks.attr.Keyspace.Value,
			Key:      k,
			Revision: r,
		}
	}
	ks.attr.Generation.Value = string(kvrevision.MarshalGeneration(gen))

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
		if _, err := xaws.Do(
			ctx,
			ks.Client.DeleteItem,
			ks.OnRequest,
			&ks.request.Delete,
		); err != nil {
			return "", convertConflictError("unable to delete keyspace pair", err)
		}

		return "", nil
	}

	if _, err := xaws.Do(
		ctx,
		ks.Client.UpdateItem,
		ks.OnRequest,
		&ks.request.Update,
	); err != nil {
		return "", convertConflictError("unable to update keyspace pair", err)
	}

	return kvrevision.IncrementGeneration(r), nil
}

func (ks *keyspace) SetUnconditional(ctx context.Context, k, v []byte) error {
	ks.attr.Key.Value = k
	ks.attr.Value.Value = v

	if len(v) == 0 {
		if _, err := xaws.Do(
			ctx,
			ks.Client.DeleteItem,
			ks.OnRequest,
			&ks.request.DeleteUnconditional,
		); err != nil {
			return fmt.Errorf("unable to delete keyspace pair: %w", err)
		}

		return nil
	}

	if _, err := xaws.Do(
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
	if err := xdynamodb.QueryRange(
		ctx,
		ks.Client,
		ks.OnRequest,
		&ks.request.Range,
		func(ctx context.Context, item map[string]types.AttributeValue) (bool, error) {
			k, err := xdynamodb.AsBytes(item, keyAttr)
			if err != nil {
				return false, err
			}

			v, err := xdynamodb.AsBytes(item, valueAttr)
			if err != nil {
				return false, err
			}

			gen, err := xdynamodb.AsUint[uint64](item, generationAttr)
			if err != nil {
				return false, err
			}

			return fn(ctx, k, v, kvrevision.MarshalGeneration(gen))
		},
	); err != nil {
		return fmt.Errorf("unable to range over keyspace: %w", err)
	}

	return nil
}

func (ks *keyspace) Close() error {
	return nil
}
