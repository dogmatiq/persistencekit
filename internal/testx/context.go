package testx

import (
	"context"
	"testing"
	"time"
)

// ContextForCleanup returns a context with a short timeout that can be used to
// cleanup after a test ends.
//
// It can be called at any time during the test (including within a cleanup
// function). The returned context will be cancelled 3 seconds after the test
// ends.
func ContextForCleanup(t testing.TB) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancelCause(context.Background())

	// Setup a channel to cancel our timer, simply to prevent a goroutine leak.
	testEnded := make(chan struct{})
	t.Cleanup(func() {
		// Signal that we're done when the test ends. It doesn't matter if
		// [ContextForCleanup] is being called before or after the test has
		// ended, cleanup functions will always be enqueued to run.
		close(testEnded)
	})

	// Setup a closure that runs our timer and cancels the context
	// after 3 seconds, unless the test has already ended.
	startTimeout := func() {
		timedOut := time.NewTimer(3 * time.Second)
		defer timedOut.Stop()

		select {
		case <-timedOut.C:
			cancel(context.DeadlineExceeded)
		case <-testEnded:
			cancel(t.Context().Err())
		}

	}

	if t.Context().Err() == nil {
		// If the test has not yet ended, don't start the timeout until it does.
		t.Cleanup(startTimeout)
	} else {
		// Otherwise, we're already in the cleanup phase, so we start the
		// timeout immediately.
		go startTimeout()
	}

	return ctx
}
