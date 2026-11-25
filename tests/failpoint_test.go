package tests

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
)

func TestFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("test cluster registers a failpoint for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		registry := testcore.FailpointRegistry(env)
		require.NotNil(t, registry, "test clusters enable failpoints with the startup switch")

		key := failpoints.MatchingForwardTask.ForScope(env.NamespaceID().String())
		require.Nil(t, registry.Get(key))
		testcore.InjectFailpoint[error](env, failpoints.MatchingForwardTask).Return(errors.New("injected"), failpoint.Once)
		require.NotNil(t, registry.Get(key))
		require.Nil(t, registry.Get(failpoints.MatchingForwardTask), "the failpoint is scoped to the test namespace")
	})
}
