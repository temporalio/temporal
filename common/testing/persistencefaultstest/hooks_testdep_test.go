//go:build test_dep

package persistencefaultstest_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/failpoint"
	"go.temporal.io/server/common/testing/persistencefaultstest"
	"go.temporal.io/server/common/testing/testhooks"
)

func TestWithFailpoints(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	updateTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "UpdateWorkflowExecution"}
	createTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "CreateWorkflowExecution"}

	testHooks := testhooks.NewTestHooks()
	key := failpoint.PersistenceKey(string(config.ExecutionStoreName), "UpdateWorkflowExecution")
	fp := failpoint.NewFailpoint[error](key)
	fp.Return(injectedErr, failpoint.Always)
	testHooks.Failpoints().Set(key, fp)

	t.Run("failpoint fails the method", func(t *testing.T) {
		t.Parallel()
		fiConfig := persistencefaultstest.WithFailpoints(nil, testHooks)
		require.ErrorIs(t, fiConfig.Injector(updateTarget), injectedErr)
		require.NoError(t, fiConfig.Injector(createTarget))
	})

	t.Run("existing injector runs first", func(t *testing.T) {
		t.Parallel()
		previousErr := errors.New("previous")
		original := &config.FaultInjection{Injector: func(target config.FaultInjectionTarget) error {
			if target.Method == "CreateWorkflowExecution" {
				return previousErr
			}
			return nil
		}}
		fiConfig := persistencefaultstest.WithFailpoints(original, testHooks)
		require.ErrorIs(t, fiConfig.Injector(createTarget), previousErr)
		require.ErrorIs(t, fiConfig.Injector(updateTarget), injectedErr)
		require.NotSame(t, original, fiConfig)
	})

	t.Run("zero test hooks leave the config unchanged", func(t *testing.T) {
		t.Parallel()
		original := &config.FaultInjection{}
		require.Same(t, original, persistencefaultstest.WithFailpoints(original, testhooks.TestHooks{}))
		require.Nil(t, persistencefaultstest.WithFailpoints(nil, testhooks.TestHooks{}))
	})
}
