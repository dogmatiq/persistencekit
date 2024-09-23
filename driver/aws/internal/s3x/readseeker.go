package s3x

import (
	"errors"
	"io"
)

// ReadSeeker is an [io.ReadSeeker] that reads from a byte slice.
//
// See https://github.com/aws/aws-sdk-go-v2/issues/1108.
type ReadSeeker struct {
	data  []byte
	begin int
}

var _ io.ReadSeeker = (*ReadSeeker)(nil)

// NewReadSeeker returns a new [ReadSeeker] that reads from the given byte
// slice.
func NewReadSeeker(data []byte) *ReadSeeker {
	r := &ReadSeeker{}
	r.Reset(data)
	return r
}

// Reset resets the reader to read from the given byte slice.
func (s *ReadSeeker) Reset(data []byte) {
	s.data = data
	s.begin = 0
}

// Read implements [io.Reader].
func (s *ReadSeeker) Read(p []byte) (n int, err error) {
	n = len(p)
	end := s.begin + n

	if end > len(s.data) {
		end = len(s.data)
		n = end - s.begin
		err = io.EOF
	}

	copy(p, s.data[s.begin:end])
	s.begin += n

	return n, err
}

// Seek implements [io.Seeker].
func (s *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.begin = int(offset)
	case io.SeekCurrent:
		s.begin += int(offset)
	case io.SeekEnd:
		s.begin = len(s.data) + int(offset)
	}

	if s.begin < 0 {
		return 0, errors.New("unable to seek to negative offset")
	}

	return int64(s.begin), nil
}
