package persistencefaultstest_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/persistencefaultstest"
	"go.temporal.io/testx/failpoint"
)

func TestWithFailpoints(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	updateTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "UpdateWorkflowExecution"}
	createTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "CreateWorkflowExecution"}

	source := failpoints.NewEnabledSource()
	key := failpoints.PersistenceKey(string(config.ExecutionStoreName), "UpdateWorkflowExecution")
	fp := failpoint.NewFailpoint[error](key)
	fp.Return(injectedErr, failpoint.Always)
	source.Failpoints().Set(key, fp)

	t.Run("failpoint fails the method", func(t *testing.T) {
		t.Parallel()
		fiConfig := persistencefaultstest.WithFailpoints(nil, source)
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
		fiConfig := persistencefaultstest.WithFailpoints(original, source)
		require.ErrorIs(t, fiConfig.Injector(createTarget), previousErr)
		require.ErrorIs(t, fiConfig.Injector(updateTarget), injectedErr)
		require.NotSame(t, original, fiConfig)
	})

	t.Run("failpoint scoped to a namespace fails only its requests", func(t *testing.T) {
		t.Parallel()
		createKey := failpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution")
		scopedSource := failpoints.NewEnabledSource()
		scopedFP := failpoint.NewFailpoint[error](createKey.ForScope("ns"))
		scopedFP.Return(injectedErr, failpoint.Always)
		scopedSource.Failpoints().Set(createKey.ForScope("ns"), scopedFP)
		fiConfig := persistencefaultstest.WithFailpoints(nil, scopedSource)

		inNamespace := createTarget
		inNamespace.NamespaceID = "ns"
		inOtherNamespace := createTarget
		inOtherNamespace.NamespaceID = "other"
		require.ErrorIs(t, fiConfig.Injector(inNamespace), injectedErr)
		require.NoError(t, fiConfig.Injector(inOtherNamespace))
		require.NoError(t, fiConfig.Injector(createTarget))
	})

	t.Run("disabled failpoints leave the config unchanged", func(t *testing.T) {
		t.Parallel()
		original := &config.FaultInjection{}
		require.Same(t, original, persistencefaultstest.WithFailpoints(original, failpoints.NewSource(failpoints.Decision{})))
		require.Nil(t, persistencefaultstest.WithFailpoints(nil, failpoints.NewSource(failpoints.Decision{})))
	})
}
