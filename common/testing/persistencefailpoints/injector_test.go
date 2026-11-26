package persistencefailpoints_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/persistencefailpoints"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

func TestNewInjector(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	updateTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "UpdateWorkflowExecution"}
	createTarget := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "CreateWorkflowExecution"}

	source := failpoint.NewRegistry()
	key := serverfailpoints.PersistenceKey(string(config.ExecutionStoreName), "UpdateWorkflowExecution")
	fp := failpoint.NewFailpoint(key)
	fp.Return(injectedErr)
	source.Set(fp)

	t.Run("failpoint fails the method", func(t *testing.T) {
		t.Parallel()
		injector := persistencefailpoints.NewInjector(source)
		require.ErrorIs(t, injector(updateTarget), injectedErr)
		require.NoError(t, injector(createTarget))
	})

	t.Run("failpoint scoped to a namespace fails only its requests", func(t *testing.T) {
		t.Parallel()
		createKey := serverfailpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution")
		scopedSource := failpoint.NewRegistry()
		scopedFP := failpoint.NewFailpoint(createKey.ForScope("ns"))
		scopedFP.Return(injectedErr)
		scopedSource.Set(scopedFP)
		injector := persistencefailpoints.NewInjector(scopedSource)

		inNamespace := createTarget
		inNamespace.Request = &persistence.GetWorkflowExecutionRequest{NamespaceID: "ns"}
		inOtherNamespace := createTarget
		inOtherNamespace.Request = &persistence.GetWorkflowExecutionRequest{NamespaceID: "other"}
		require.ErrorIs(t, injector(inNamespace), injectedErr)
		require.NoError(t, injector(inOtherNamespace))
		require.NoError(t, injector(createTarget), "a request without a namespace has no scope")
	})

	t.Run("failpoint registered after the injector was created fails the method", func(t *testing.T) {
		t.Parallel()
		lateSource := failpoint.NewRegistry()
		injector := persistencefailpoints.NewInjector(lateSource)
		require.NoError(t, injector(updateTarget))

		lateFP := failpoint.NewFailpoint(key)
		lateFP.Return(injectedErr)
		lateSource.Set(lateFP)
		require.ErrorIs(t, injector(updateTarget), injectedErr)

		lateFP.Disable()
		lateSource.CompareAndDelete(lateFP)
		require.NoError(t, injector(updateTarget))
	})

	t.Run("no injector without a registry", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, persistencefailpoints.NewInjector(nil))
	})
}

// TestNewInjector_Buggify checks that BUGGIFY, installed as the registry's Policy, only fails
// a persistence method if its configuration selects the persistence keys and has an error.
func TestNewInjector_Buggify(t *testing.T) {
	t.Parallel()

	target := config.FaultInjectionTarget{Store: config.ExecutionStoreName, Method: "UpdateWorkflowExecution"}
	for _, tc := range []struct {
		name    string
		config  buggify.Config
		wantErr bool
	}{
		{
			name: "persistence keys matched with an error",
			config: buggify.Config{Seed: 1, Keys: []buggify.Key{
				{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"},
			}},
			wantErr: true,
		},
		{
			name: "persistence keys matched by no entry",
			config: buggify.Config{Seed: 1, Keys: []buggify.Key{
				{Match: "grpc:*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"},
			}},
		},
		{
			name: "persistence keys kept out by an entry before the broad one",
			config: buggify.Config{Seed: 1, Keys: []buggify.Key{
				{Match: "persistence:*"},
				{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"},
			}},
		},
		{
			name:   "persistence keys matched without an error",
			config: buggify.Config{Seed: 1, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 1}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			b := buggify.New(source)
			require.NoError(t, b.Set(tc.config))

			err := persistencefailpoints.NewInjector(source)(target)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
