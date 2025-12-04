package s3journal

import (
	"context"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/internal/s3x"
	"github.com/dogmatiq/persistencekit/internal/x/xsync"
	"github.com/dogmatiq/persistencekit/journal"
)

// store is an implementation of [journal.BinaryStore] that persists to an S3
// bucket.
type store struct {
	Client    *s3.Client
	Bucket    string
	OnRequest func(any) []func(*s3.Options)

	createBucketOnce xsync.SucceedOnce
}

// NewBinaryStore returns a new [journal.BinaryStore] that uses the given
// S3 client to store journal records in the given bucket.
func NewBinaryStore(
	client *s3.Client,
	bucket string,
	options ...Option,
) journal.BinaryStore {
	s := &store{
		Client: client,
		Bucket: bucket,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Option is a functional option that changes the behavior of [NewBinaryStore].
type Option func(*store)

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
	return func(s *store) {
		s.OnRequest = fn
	}
}

// Open returns the journal with the given name.
func (s *store) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	if s.Bucket == "" {
		panic("bucket name must not be empty")
	}

	if err := s.createBucketOnce.Do(ctx, s.createBucket); err != nil {
		return nil, err
	}

	j := &journ{
		client:          s.Client,
		onRequest:       s.OnRequest,
		name:            name,
		bucket:          s.Bucket,
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

func (s *store) createBucket(ctx context.Context) error {
	return s3x.CreateBucketIfNotExists(
		ctx,
		s.Client,
		s.Bucket,
		s.OnRequest,
	)
}
