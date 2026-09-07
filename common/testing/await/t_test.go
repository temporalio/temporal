package await_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestT_CollectsAssertionFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fail  func(*await.T)
		stops bool
	}{
		{
			name: "assert",
			fail: func(t *await.T) {
				assert.Equal(t, "expected", "actual") //nolint:forbidigo // intentionally testing that assert.* works with *await.T
			},
		},
		{
			name: "Errorf",
			fail: func(t *await.T) {
				t.Error("not ready")
			},
		},
		{
			name: "FailNow",
			fail: func(t *await.T) {
				t.FailNow()
			},
			stops: true,
		},
		{
			name: "Fatal",
			fail: func(t *await.T) {
				t.Fatal("not ready")
			},
			stops: true,
		},
		{
			name: "Fatalf",
			fail: func(t *await.T) {
				t.Fatalf("not ready: %d", 1)
			},
			stops: true,
		},
		{
			name: "require",
			fail: func(t *await.T) {
				require.Equal(t, "expected", "actual")
			},
			stops: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			at := &await.T{}
			continuedAfterFailure := false
			run := func() {
				tc.fail(at)
				continuedAfterFailure = true
			}

			if tc.stops {
				require.Panics(t, run)
				require.False(t, continuedAfterFailure)
			} else {
				require.NotPanics(t, run)
				require.True(t, continuedAfterFailure)
			}
			require.True(t, at.Failed())
		})
	}
}
