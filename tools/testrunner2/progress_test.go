package testrunner2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkTracker(t *testing.T) {
	t.Parallel()

	t.Run("does not count failed roots as successful", func(t *testing.T) {
		t.Parallel()

		var tracker workTracker
		tracker.reset()
		tracker.addRoots(2)

		tracker.beginAttempt("failed")
		update := tracker.finishAttempt("failed", false)
		require.Equal(t, progressUpdate{completed: 0, total: 2}, update)

		tracker.beginAttempt("passed")
		update = tracker.finishAttempt("passed", true)
		require.Equal(t, progressUpdate{completed: 1, total: 2, done: true}, update)
	})

	t.Run("counts a root after all retry work finishes", func(t *testing.T) {
		t.Parallel()

		var tracker workTracker
		tracker.reset()
		tracker.addRoots(1)

		tracker.beginAttempt("root")
		tracker.beginAttempt("root")

		update := tracker.finishAttempt("root", true)
		require.Equal(t, progressUpdate{completed: 0, total: 1}, update)

		update = tracker.finishAttempt("root", true)
		require.Equal(t, progressUpdate{completed: 1, total: 1, done: true}, update)
	})
}
