package xs3

import (
	"errors"

	"github.com/aws/smithy-go"
)

// IsNotExists returns true if err is an error that indicates the requested
// object was not found.
func IsNotExists(err error) bool {
	var e smithy.APIError
	if errors.As(err, &e) {
		switch e.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
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
	var e smithy.APIError
	if errors.As(err, &e) {
		switch e.ErrorCode() {
		case "BucketAlreadyExists", "BucketAlreadyOwnedByYou":
			return true
		}
	}

	return false
}

// IsConflict returns true if err is an error that indicates an object conflict.
func IsConflict(err error) bool {
	var e smithy.APIError
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
