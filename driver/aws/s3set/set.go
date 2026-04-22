package s3set

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/awsx"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/internal/x/xerrors"
	"github.com/dogmatiq/persistencekit/set"
)

// memberBody is the content written to live member objects.
//
// Tombstones are size-zero objects (tagged for lifecycle expiry); live members
// carry a non-zero body so they can be distinguished from tombstones.
var memberBody = []byte("X")

// setimpl is an implementation of [set.BinarySet] that persists to an S3
// bucket.
type setimpl struct {
	client    *s3.Client
	onRequest func(any) []func(*s3.Options)

	// name is the set name.
	name string

	// bucket is the name of the S3 bucket in which the set's members are
	// stored.
	bucket string

	// objectKeyPrefix is the string prepended to the key of each S3 object. It
	// includes the set's name, allowing objects for multiple sets to be stored
	// in the same bucket.
	objectKeyPrefix string
}

func (s *setimpl) Name() string {
	return s.name
}

func (s *setimpl) Has(ctx context.Context, member []byte) (_ bool, err error) {
	defer xerrors.Wrap(&err, "unable to check membership in the %q set", s.name)

	key := s.objectKey(member)

	res, err := awsx.Do(
		ctx,
		s.client.HeadObject,
		s.onRequest,
		&s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		},
	)
	if s3x.IsNotExists(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return aws.ToInt64(res.ContentLength) > 0, nil
}

func (s *setimpl) Add(ctx context.Context, member []byte) (err error) {
	defer xerrors.Wrap(&err, "unable to add member to the %q set", s.name)

	key := s.objectKey(member)

	_, err = awsx.Do(
		ctx,
		s.client.PutObject,
		s.onRequest,
		&s3.PutObjectInput{
			Bucket:        &s.bucket,
			Key:           &key,
			Body:          s3x.NewReadSeeker(memberBody),
			ContentLength: aws.Int64(int64(len(memberBody))),
		},
	)
	return err
}

func (s *setimpl) TryAdd(ctx context.Context, member []byte) (_ bool, err error) {
	defer xerrors.Wrap(&err, "unable to add member to the %q set", s.name)

	key := s.objectKey(member)

	for {
		_, err := awsx.Do(
			ctx,
			s.client.PutObject,
			s.onRequest,
			&s3.PutObjectInput{
				Bucket:        &s.bucket,
				Key:           &key,
				IfNoneMatch:   aws.String("*"),
				Body:          s3x.NewReadSeeker(memberBody),
				ContentLength: aws.Int64(int64(len(memberBody))),
			},
		)
		if err == nil {
			return true, nil
		}
		if !s3x.IsConflict(err) {
			return false, err
		}

		// Something occupies the slot. Check if it is a tombstone.
		existingETag, size, err := s.headObject(ctx, key)
		if err != nil {
			return false, err
		}
		if existingETag == "" {
			// Object vanished between PutObject and HeadObject; retry.
			continue
		}
		if size > 0 {
			// Already a live member.
			return false, nil
		}

		// Replace the tombstone with a live member.
		_, err = awsx.Do(
			ctx,
			s.client.PutObject,
			s.onRequest,
			&s3.PutObjectInput{
				Bucket:        &s.bucket,
				Key:           &key,
				IfMatch:       aws.String(existingETag),
				Body:          s3x.NewReadSeeker(memberBody),
				ContentLength: aws.Int64(int64(len(memberBody))),
			},
		)
		if err == nil {
			return true, nil
		}
		if !s3x.IsConflict(err) && !s3x.IsNotExists(err) {
			return false, err
		}
		// Tombstone was replaced or removed concurrently; retry from the top.
	}
}

func (s *setimpl) Remove(ctx context.Context, member []byte) (err error) {
	defer xerrors.Wrap(&err, "unable to remove member from the %q set", s.name)

	key := s.objectKey(member)

	_, err = awsx.Do(
		ctx,
		s.client.PutObject,
		s.onRequest,
		&s3.PutObjectInput{
			Bucket:        &s.bucket,
			Key:           &key,
			Body:          s3x.NewReadSeeker(nil),
			ContentLength: aws.Int64(0),
			Tagging:       s3x.TombstoneTagging,
		},
	)
	return err
}

func (s *setimpl) TryRemove(ctx context.Context, member []byte) (_ bool, err error) {
	defer xerrors.Wrap(&err, "unable to remove member from the %q set", s.name)

	key := s.objectKey(member)

	for {
		existingETag, size, err := s.headObject(ctx, key)
		if err != nil {
			return false, err
		}
		if size == 0 {
			// Absent or already a tombstone.
			return false, nil
		}

		// Live member found; replace with a tombstone.
		_, err = awsx.Do(
			ctx,
			s.client.PutObject,
			s.onRequest,
			&s3.PutObjectInput{
				Bucket:        &s.bucket,
				Key:           &key,
				IfMatch:       aws.String(existingETag),
				Body:          s3x.NewReadSeeker(nil),
				ContentLength: aws.Int64(0),
				Tagging:       s3x.TombstoneTagging,
			},
		)
		if err == nil {
			return true, nil
		}
		if !s3x.IsConflict(err) && !s3x.IsNotExists(err) {
			return false, err
		}
		// Member was modified concurrently; retry.
	}
}

func (s *setimpl) Range(ctx context.Context, fn set.BinaryRangeFunc) (err error) {
	defer xerrors.Wrap(&err, "unable to range over the %q set", s.name)

	req := &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: aws.String(s.objectKeyPrefix),
	}

	for {
		list, err := awsx.Do(
			ctx,
			s.client.ListObjectsV2,
			s.onRequest,
			req,
		)
		if err != nil {
			return err
		}

		for _, obj := range list.Contents {
			if aws.ToInt64(obj.Size) == 0 {
				continue // tombstone
			}

			member, err := s.decodeMember(aws.ToString(obj.Key))
			if err != nil {
				return err
			}

			ok, err := fn(ctx, member)
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

func (s *setimpl) Close() error {
	return nil
}

// objectKey returns the S3 object key for the given set member.
func (s *setimpl) objectKey(member []byte) string {
	return s.objectKeyPrefix + hex.EncodeToString(member)
}

// decodeMember extracts member bytes from a full S3 object key.
func (s *setimpl) decodeMember(s3Key string) ([]byte, error) {
	suffix, ok := strings.CutPrefix(s3Key, s.objectKeyPrefix)
	if !ok {
		return nil, fmt.Errorf("malformed object key %q: expected prefix %q", s3Key, s.objectKeyPrefix)
	}

	member, err := hex.DecodeString(suffix)
	if err != nil {
		return nil, fmt.Errorf("malformed object key %q: %w", s3Key, err)
	}
	return member, nil
}

// headObject returns the ETag and size of the object at key.
// If the object does not exist, etag is "".
func (s *setimpl) headObject(ctx context.Context, key string) (etag string, size int64, err error) {
	res, err := awsx.Do(
		ctx,
		s.client.HeadObject,
		s.onRequest,
		&s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		},
	)
	if s3x.IsNotExists(err) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	return aws.ToString(res.ETag), aws.ToInt64(res.ContentLength), nil
}
