package s3x

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// IsNotExists returns true if err is an error that indicates the requested
// object was not found.
func IsNotExists(err error) bool {
	if err == nil {
		return false
	}

	for err != nil {
		switch err.(type) {
		case *types.NotFound:
			return true
		case *types.NoSuchKey:
			return true
		case *types.NoSuchBucket:
			return true
		default:
			err = errors.Unwrap(err)
		}
	}

	return false
}

// IgnoreNotExists returns nil if err is an error that indicates the requested
// object was not found; otherwise it returns err.
func IgnoreNotExists(err error) error {
	if IsNotExists(err) {
		return nil
	}
	return err
}

// IsAlreadyExists returns true if err is an error that indicates the requested
// object already exists.
func IsAlreadyExists(err error) bool {
	if err == nil {
		return false
	}

	for err != nil {
		switch err.(type) {
		case *types.BucketAlreadyExists:
			return true
		case *types.BucketAlreadyOwnedByYou:
			return true
		default:
			err = errors.Unwrap(err)
		}
	}

	return false
}

// IsConflict returns true if err is an error that indicates an object conflict.
func IsConflict(err error) bool {
	var e *smithy.GenericAPIError
	if errors.As(err, &e) {
		return e.ErrorCode() == "PreconditionFailed"
	}
	return false
}

// IgnoreAlreadyExists returns nil if err is an error that indicates the
// requested object already exists; otherwise it returns err.
func IgnoreAlreadyExists(err error) error {
	if IsAlreadyExists(err) {
		return nil
	}
	return err
}
