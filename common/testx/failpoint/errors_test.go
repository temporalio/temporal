package failpoint_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

func TestErrorCatalog(t *testing.T) {
	t.Parallel()

	injected := errors.New("base")
	// The catalog is global, so each run registers a name of its own.
	name := "TestErrorCatalog_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return errors.Join(injected, errors.New(msg))
	})

	newError, ok := failpoint.LookupError(name)
	require.True(t, ok)
	err := newError("detail")
	require.ErrorIs(t, err, injected)
	require.ErrorContains(t, err, "detail")

	_, ok = failpoint.LookupError(name + "_Unknown")
	require.False(t, ok)

	require.Contains(t, failpoint.ErrorNames(), name)
	require.Panics(t, func() {
		failpoint.RegisterError(name, func(string) error { return nil })
	})
}
