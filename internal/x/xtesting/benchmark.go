package xtesting

import (
	"context"
	"testing"
	"time"
)

// Benchmark benchmarks fn.
//
// It calls the setup function once before the first iteration of the benchmark.
//
// The pre and post functions before and after each iteration of the benchmark,
// respectively.
//
// Only the time spent in fn is measured.
func Benchmark(
	b *testing.B,
	setup func(context.Context) error,
	pre func(context.Context) error,
	fn func(context.Context) error,
	post func(context.Context) error,
) {
	ctx := b.Context()

	const timeout = 30 * time.Second
	checkIterationThreshold(b)

	if setup != nil {
		setupCtx, cancel := context.WithTimeout(ctx, timeout)
		err := setup(setupCtx)
		cancel()

		if err != nil {
			b.Fatal(err)
		}
	}

	for b.Loop() {
		b.StopTimer()

		if pre != nil {
			preCtx, cancel := context.WithTimeout(ctx, timeout)
			err := pre(preCtx)
			cancel()

			if err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		err := fn(ctx)
		b.StopTimer()

		if post != nil {
			postCtx, cancel := context.WithTimeout(ctx, timeout)
			err := post(postCtx)
			cancel()

			if err != nil {
				b.Fatal(err)
			}
		}

		if err != nil {
			b.Fatal(err)
		}
	}
}

// checkIterationThreshold skips the benchmark if the number of iterations is
// too high.
//
// This usually occurs when the benchmarking framework is unable to measure the
// duration of each iteration, typically because the benchmarked code is "too
// fast".
func checkIterationThreshold(b *testing.B) {
	const threshold = 1_000_000
	if b.N >= threshold {
		b.Skipf("benchmark skipped, too many iterations (%d); benchmarked code is likely too fast to measure meaningfully", b.N)
	}
}
