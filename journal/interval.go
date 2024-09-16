package journal

import (
	"fmt"
	"iter"
)

// Interval describes a half-open interval of positions in a [Journal].
type Interval struct {
	// Begin is the position of the first record in the interval.
	Begin Position

	// End is the position immediately after the last record in the interval.
	End Position
}

// IsEmpty returns true if the interval contains no records.
func (i Interval) IsEmpty() bool {
	return i.Begin >= i.End
}

// Len returns the number of records in the interval.
func (i Interval) Len() int {
	if i.Begin < i.End {
		return int(i.End - i.Begin)
	}
	return 0
}

// Contains returns true if the interval contains the given position.
func (i Interval) Contains(pos Position) bool {
	return i.Begin <= pos && pos < i.End
}

// Positions returns a sequence of all positions in the interval.
func (i Interval) Positions() iter.Seq2[int, Position] {
	return func(yield func(int, Position) bool) {
		pos := i.Begin

		for index := range i.Len() {
			if !yield(index, pos) {
				return
			}
			pos++
		}
	}
}

func (i Interval) String() string {
	return fmt.Sprintf("[%d, %d)", i.Begin, i.End)
}
