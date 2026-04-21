package s3kv

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/internal/x/xerrors"
	"github.com/dogmatiq/persistencekit/kv"
)

const (
	// tombstoneTagging is the URL-encoded tag string applied to tombstone objects.
	// It is used as a lifecycle rule filter to automatically expire tombstones.
	tombstoneTagging = "type=tombstone"

	// tombstoneMetaKey is the metadata key used to identify tombstone objects
	// without requiring a separate GetObjectTagging call.
	tombstoneMetaKey = "tombstone"
)

// isTombstone returns true if the given object metadata indicates a tombstone.
func isTombstone(metadata map[string]string) bool {
	return metadata[tombstoneMetaKey] == "1"
}

// keyspace is an implementation of [kv.BinaryKeyspace] that persists to an S3
// bucket.
type keyspace struct {
	client    *s3.Client
	onRequest func(any) []func(*s3.Options)

	// name is the keyspace name.
	name string

	// bucket is the name of the S3 bucket in which the keyspace's key/value
	// pairs are stored.
	bucket string

	// objectKeyPrefix is the string prepended to the key of each S3 object. It
	// includes the keyspace's name, allowing objects for multiple keyspaces to
	// be stored in the same bucket.
	objectKeyPrefix string
}

func (ks *keyspace) Name() string {
	return ks.name
}

func (ks *keyspace) Get(ctx context.Context, k []byte) (v []byte, r kv.Revision, err error) {
	defer xerrors.Wrap(&err, "unable to get pair from the %q keyspace", ks.name)

	key := ks.objectKey(k)

	res, err := awsx.Do(
		ctx,
		ks.client.GetObject,
		ks.onRequest,
		&s3.GetObjectInput{
			Bucket: &ks.bucket,
			Key:    &key,
		},
	)
	if s3x.IsNotExists(err) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}

	isTomb := isTombstone(res.Metadata)
	etag := aws.ToString(res.ETag)

	v, err = io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, "", err
	}

	if isTomb {
		return nil, "", nil
	}

	return v, kv.Revision(etag), nil
}

func (ks *keyspace) Has(ctx context.Context, k []byte) (_ bool, err error) {
	defer xerrors.Wrap(&err, "unable to check pair in the %q keyspace", ks.name)

	key := ks.objectKey(k)

	res, err := awsx.Do(
		ctx,
		ks.client.HeadObject,
		ks.onRequest,
		&s3.HeadObjectInput{
			Bucket: &ks.bucket,
			Key:    &key,
		},
	)
	if s3x.IsNotExists(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return !isTombstone(res.Metadata), nil
}

func (ks *keyspace) Set(ctx context.Context, k, v []byte, r kv.Revision) (_ kv.Revision, err error) {
	defer xerrors.Wrap(&err, "unable to set pair in the %q keyspace", ks.name)

	if len(v) == 0 {
		return ks.setDelete(ctx, k, r)
	}
	return ks.setWrite(ctx, k, v, r)
}

// setWrite handles Set when v is non-nil (an insert or update).
func (ks *keyspace) setWrite(ctx context.Context, k, v []byte, r kv.Revision) (kv.Revision, error) {
	key := ks.objectKey(k)

	if r != "" {
		// Conditional update: replace only if the current ETag matches r.
		out, err := awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				IfMatch:       aws.String(string(r)),
				Body:          s3x.NewReadSeeker(v),
				ContentLength: aws.Int64(int64(len(v))),
			},
		)
		if s3x.IsConflict(err) || s3x.IsNotExists(err) {
			return "", kv.ConflictError[[]byte]{Keyspace: ks.name, Key: k, Revision: r}
		}
		if err != nil {
			return "", err
		}
		return kv.Revision(aws.ToString(out.ETag)), nil
	}

	// Insert: the key must not exist or must currently be a tombstone.
	for {
		out, err := awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				IfNoneMatch:   aws.String("*"),
				Body:          s3x.NewReadSeeker(v),
				ContentLength: aws.Int64(int64(len(v))),
			},
		)
		if err == nil {
			return kv.Revision(aws.ToString(out.ETag)), nil
		}
		if !s3x.IsConflict(err) {
			return "", err
		}

		// Something is occupying the slot. Check if it is a tombstone.
		existingETag, isTomb, err := ks.headObject(ctx, key)
		if err != nil {
			return "", err
		}
		if existingETag == "" {
			// Object vanished between PutObject and HeadObject; retry insert.
			continue
		}
		if !isTomb {
			return "", kv.ConflictError[[]byte]{Keyspace: ks.name, Key: k, Revision: r}
		}

		// Replace the tombstone with the real value.
		out, err = awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				IfMatch:       aws.String(existingETag),
				Body:          s3x.NewReadSeeker(v),
				ContentLength: aws.Int64(int64(len(v))),
			},
		)
		if err == nil {
			return kv.Revision(aws.ToString(out.ETag)), nil
		}
		if !s3x.IsConflict(err) {
			return "", err
		}
		// Tombstone was replaced concurrently; retry from the top.
	}
}

// setDelete handles Set when v is nil (a delete or tombstone write).
func (ks *keyspace) setDelete(ctx context.Context, k []byte, r kv.Revision) (kv.Revision, error) {
	key := ks.objectKey(k)

	if r != "" {
		// Conditional delete: write tombstone only if the current ETag matches r.
		_, err := awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				IfMatch:       aws.String(string(r)),
				Body:          s3x.NewReadSeeker(nil),
				ContentLength: aws.Int64(0),
				Metadata:      map[string]string{tombstoneMetaKey: "1"},
				Tagging:       aws.String(tombstoneTagging),
			},
		)
		if s3x.IsConflict(err) || s3x.IsNotExists(err) {
			return "", kv.ConflictError[[]byte]{Keyspace: ks.name, Key: k, Revision: r}
		}
		if err != nil {
			return "", err
		}
		return "", nil
	}

	// Delete with no revision: the key must not exist (or already be a tombstone).
	for {
		_, err := awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				IfNoneMatch:   aws.String("*"),
				Body:          s3x.NewReadSeeker(nil),
				ContentLength: aws.Int64(0),
				Metadata:      map[string]string{tombstoneMetaKey: "1"},
				Tagging:       aws.String(tombstoneTagging),
			},
		)
		if err == nil {
			return "", nil
		}
		if !s3x.IsConflict(err) {
			return "", err
		}

		// Something is occupying the slot. Check what it is.
		existingETag, isTomb, err := ks.headObject(ctx, key)
		if err != nil {
			return "", err
		}
		if existingETag == "" || isTomb {
			// Key is absent or already a tombstone — intent satisfied.
			return "", nil
		}
		return "", kv.ConflictError[[]byte]{Keyspace: ks.name, Key: k, Revision: r}
	}
}

// headObject returns the ETag and tombstone status of the object at key.
// If the object does not exist, etag is "" and isTomb is false.
func (ks *keyspace) headObject(ctx context.Context, key string) (etag string, isTomb bool, err error) {
	res, err := awsx.Do(
		ctx,
		ks.client.HeadObject,
		ks.onRequest,
		&s3.HeadObjectInput{
			Bucket: &ks.bucket,
			Key:    &key,
		},
	)
	if s3x.IsNotExists(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return aws.ToString(res.ETag), isTombstone(res.Metadata), nil
}

func (ks *keyspace) SetUnconditional(ctx context.Context, k, v []byte) (err error) {
	defer xerrors.Wrap(&err, "unable to set pair unconditionally in the %q keyspace", ks.name)

	key := ks.objectKey(k)

	if len(v) == 0 {
		_, err := awsx.Do(
			ctx,
			ks.client.PutObject,
			ks.onRequest,
			&s3.PutObjectInput{
				Bucket:        &ks.bucket,
				Key:           &key,
				Body:          s3x.NewReadSeeker(nil),
				ContentLength: aws.Int64(0),
				Metadata:      map[string]string{tombstoneMetaKey: "1"},
				Tagging:       aws.String(tombstoneTagging),
			},
		)
		if err != nil {
			return err
		}
		return nil
	}

	_, err = awsx.Do(
		ctx,
		ks.client.PutObject,
		ks.onRequest,
		&s3.PutObjectInput{
			Bucket:        &ks.bucket,
			Key:           &key,
			Body:          s3x.NewReadSeeker(v),
			ContentLength: aws.Int64(int64(len(v))),
		},
	)
	if err != nil {
		return err
	}
	return nil
}

func (ks *keyspace) Range(ctx context.Context, fn kv.BinaryRangeFunc) (err error) {
	defer xerrors.Wrap(&err, "unable to range over the %q keyspace", ks.name)

	req := &s3.ListObjectsV2Input{
		Bucket: &ks.bucket,
		Prefix: aws.String(ks.objectKeyPrefix),
	}

	for {
		list, err := awsx.Do(
			ctx,
			ks.client.ListObjectsV2,
			ks.onRequest,
			req,
		)
		if err != nil {
			return err
		}

		for _, obj := range list.Contents {
			s3Key := aws.ToString(obj.Key)

			k, err := ks.decodeObjectKey(s3Key)
			if err != nil {
				return err
			}

			res, err := awsx.Do(
				ctx,
				ks.client.GetObject,
				ks.onRequest,
				&s3.GetObjectInput{
					Bucket: &ks.bucket,
					Key:    &s3Key,
				},
			)
			if s3x.IsNotExists(err) {
				continue // Deleted between list and get.
			}
			if err != nil {
				return err
			}

			isTomb := isTombstone(res.Metadata)
			etag := aws.ToString(res.ETag)

			v, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				return err
			}

			if isTomb {
				continue
			}

			ok, err := fn(ctx, k, v, kv.Revision(etag))
			if !ok || err != nil {
				return err
			}
		}

		if list.IsTruncated == nil || !*list.IsTruncated {
			return nil
		}
		req.ContinuationToken = list.NextContinuationToken
	}
}

func (ks *keyspace) Close() error {
	return nil
}

// objectKey returns the S3 object key for the given KV key.
func (ks *keyspace) objectKey(k []byte) string {
	return ks.objectKeyPrefix + hex.EncodeToString(k)
}

// decodeObjectKey extracts the KV key bytes from a full S3 object key.
func (ks *keyspace) decodeObjectKey(s3Key string) ([]byte, error) {
	k, err := hex.DecodeString(s3Key[len(ks.objectKeyPrefix):])
	if err != nil {
		return nil, fmt.Errorf("malformed object key %q: %w", s3Key, err)
	}
	return k, nil
}
