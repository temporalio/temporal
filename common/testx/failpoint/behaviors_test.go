package failpoint_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

func TestBehaviorCatalog(t *testing.T) {
	t.Parallel()

	injected := errors.New("behavior")
	// The catalog is global, so each run registers a name of its own.
	name := "TestBehaviorCatalog_" + rand.Text()
	failpoint.RegisterBehavior(name, func() error { return injected })

	behavior, ok := failpoint.LookupBehavior(name)
	require.True(t, ok)
	require.ErrorIs(t, behavior(), injected)

	_, ok = failpoint.LookupBehavior(name + "_Unknown")
	require.False(t, ok)

	require.Contains(t, failpoint.BehaviorNames(), name)
	require.Panics(t, func() {
		failpoint.RegisterBehavior(name, func() error { return nil })
	})
}
