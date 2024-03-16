package bigint_test

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	. "github.com/dogmatiq/persistencekit/driver/sql/postgres/internal/bigint"
)

func TestConvertUnsigned(t *testing.T) {
	cases := []struct {
		Name     string
		Unsigned uint64
		Signed   int64
	}{
		{
			"zero",
			0,
			math.MinInt64,
		},
		{
			"mid-point",
			(math.MaxUint64 / 2) + 1,
			0,
		},
		{
			"max uint64",
			math.MaxUint64,
			math.MaxInt64,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			s, err := ConvertUnsigned(&c.Unsigned).Value()
			if err != nil {
				t.Fatal(err)
			}

			if s != c.Signed {
				t.Fatalf("unexpected encoded value: got %d, want %d", s, c.Signed)
			}

			var u uint64
			if err := ConvertUnsigned(&u).Scan(c.Signed); err != nil {
				t.Fatal(err)
			}

			if u != c.Unsigned {
				t.Fatalf("unexpected decoded value: got %d, want %d", u, c.Unsigned)
			}
		})
	}

	var (
		unsigned []uint64
		signed   []int64
	)

	// Test encoding/decoding of random values produces the original value.
	for i := 0; i < 1000; i++ {
		random := rand.Uint64()

		s, err := ConvertUnsigned(&random).Value()
		if err != nil {
			t.Fatal(err)
		}

		var u uint64
		if err := ConvertUnsigned(&u).Scan(s); err != nil {
			t.Fatal(err)
		}

		if u != random {
			t.Fatalf("unexpected decoded value: got %d, want %d", u, random)
		}

		unsigned = append(unsigned, u)
		signed = append(signed, s.(int64))
	}

	// Then sort those values and verify that the order is preserved.
	slices.Sort(unsigned)
	slices.Sort(signed)

	for i, u := range unsigned {
		s := signed[i]

		x, err := ConvertUnsigned(&u).Value()
		if err != nil {
			t.Fatal(err)
		}

		if x != s {
			t.Fatalf("unexpected encoded value at index %d: got %d, want %d", i, x, s)
		}
	}
}
