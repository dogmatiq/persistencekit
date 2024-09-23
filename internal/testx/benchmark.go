package testx

import (
	"context"
	"testing"
	"time"
)

// Benchmark benchmarks fn.
func Benchmark(
	b *testing.B,
	setup func(context.Context) error,
	before func(context.Context) error,
	fn func(context.Context) error,
	after func(context.Context) error,
) {
	b.StopTimer()
	checkIterationThreshold(b)

	if setup != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := setup(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		if before != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := before(ctx)
			cancel()
			if err != nil {
				b.Fatal(err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		b.StartTimer()
		err := fn(ctx)
		b.StopTimer()

		cancel()

		if err != nil {
			b.Fatal(err)
		}

		if after != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := after(ctx)
			cancel()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// checkIterationThreshold skips the benchmark if the number of iterations is
// too high. This usually occurs when the benchmarking framework is unable to
// measure the duration of each iteration, typically because the benchmarked
// code is "too fast".
func checkIterationThreshold(b *testing.B) {
	const threshold = 1_000_000
	if b.N >= threshold {
		b.Skipf("benchmark skipped, too many iterations (%d); benchmarked code is likely too fast to measure meaningfully", b.N)
	}
}
