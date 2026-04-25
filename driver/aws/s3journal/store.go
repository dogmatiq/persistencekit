package s3journal

import (
	"context"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/journal"
)

// BinaryStore is an implementation of [journal.BinaryStore] that persists to an S3
// bucket.
type BinaryStore struct {
	client        *s3.Client
	bucket        string
	onRequest     func(any) []func(*s3.Options)
	provisionOnce xsync.SucceedOnce
}

// NewBinaryStore returns a new [journal.BinaryStore] that uses the given
// S3 client to store journal records in the given bucket.
func NewBinaryStore(
	client *s3.Client,
	bucket string,
	options ...Option,
) *BinaryStore {
	s := &BinaryStore{
		client: client,
		bucket: bucket,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Option is a functional option that changes the behavior of [NewBinaryStore].
type Option func(*BinaryStore)

// WithRequestHook is an [Option] that configures fn as a pre-request hook.
//
// Before each S3 API request, fn is passed a pointer to the input struct, e.g.
// [s3.GetObjectInput], which it may modify in-place. It may be called with any
// S3 request type. The types of requests used may change in any version without
// notice.
//
// Any functions returned by fn will be applied to the request's options before
// the request is sent.
func WithRequestHook(fn func(any) []func(*s3.Options)) Option {
	return func(s *BinaryStore) {
		s.onRequest = fn
	}
}

// Provision creates the S3 bucket used by the store if it does not already
// exist.
//
// The store also creates the bucket on first use if it does not exist.
// Provision allows infrastructure to be created ahead of time, for example as
// part of a deployment pipeline, so that the application itself does not need
// broad IAM permissions.
func (s *BinaryStore) Provision(ctx context.Context) error {
	return s.provisionOnce.Do(ctx, func(ctx context.Context) error {
		_, err := s3x.CreateBucketIfNotExists(ctx, s.client, s.bucket, s.onRequest)
		return err
	})
}

// Open returns the journal with the given name.
func (s *BinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	if s.bucket == "" {
		panic("bucket name must not be empty")
	}

	if err := s.Provision(ctx); err != nil {
		return nil, err
	}

	j := &journ{
		client:          s.client,
		onRequest:       s.onRequest,
		name:            name,
		bucket:          s.bucket,
		objectKeyPrefix: "journal/" + url.PathEscape(name) + "/",
	}

	if err := j.refresh(ctx); err != nil {
		return nil, err
	}

	if err := j.compact(ctx); err != nil {
		return nil, err
	}

	return j, nil
}
