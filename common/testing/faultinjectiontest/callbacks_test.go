package faultinjectiontest_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var callbackKey = failpoint.KeyFromName("callback", "namespace-id")

func TestAddCallback_RunsInOrderUntilError(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	injected := errors.New("injected")
	var calls []string
	add := func(name string, err error) func() {
		remove, addErr := faultinjectiontest.AddCallback(registry, callbackKey, func(arg any) error {
			require.Equal(t, "arg", arg)
			calls = append(calls, name)
			return err
		})
		require.NoError(t, addErr)
		t.Cleanup(remove)
		return remove
	}
	add("first", nil)
	removeSecond := add("second", injected)
	add("third", errors.New("not reached"))

	err, fired := failpoint.ExecuteWith[error](src, callbackKey, "arg")
	require.True(t, fired)
	require.Equal(t, injected, err)
	require.Equal(t, []string{"first", "second"}, calls)

	calls = nil
	removeSecond()
	_, _ = failpoint.ExecuteWith[error](src, callbackKey, "arg")
	require.Equal(t, []string{"first", "third"}, calls)
}

func TestAddCallback_LastRemoveRemovesFailpoint(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	removeFirst, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
	require.NoError(t, err)
	removeSecond, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
	require.NoError(t, err)
	fp := registry.Get(callbackKey)
	require.NotNil(t, fp)

	removeFirst()
	removeFirst()
	require.Same(t, fp, registry.Get(callbackKey))

	removeSecond()
	require.Nil(t, registry.Get(callbackKey))
	require.False(t, fp.(*failpoint.Failpoint[error]).Info().Enabled)

	// A new callback registers a new failpoint.
	removeThird, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
	require.NoError(t, err)
	defer removeThird()
	require.NotNil(t, registry.Get(callbackKey))
	require.NotSame(t, fp, registry.Get(callbackKey))
}

func TestAddCallback_ScopesAndRegistriesAreSeparate(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	otherRegistry := failpoint.NewRegistry()
	otherScope := callbackKey.ForScope("other")
	for _, target := range []struct {
		registry failpoint.Registry
		key      failpoint.Key
	}{{registry, callbackKey}, {registry, otherScope}, {otherRegistry, callbackKey}} {
		remove, err := faultinjectiontest.AddCallback(target.registry, target.key, func(any) error { return nil })
		require.NoError(t, err)
		t.Cleanup(remove)
	}
	require.NotSame(t, registry.Get(callbackKey), registry.Get(otherScope))
	require.NotSame(t, registry.Get(callbackKey), otherRegistry.Get(callbackKey))
}

func TestAddCallback_OtherFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("registered before", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		other := failpoint.NewFailpoint[error](callbackKey)
		registry.Set(callbackKey, other)

		_, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
		require.EqualError(t, err, "failpoint callback@namespace-id is already registered")
		require.Same(t, other, registry.Get(callbackKey))
	})

	t.Run("replaced", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		remove, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
		require.NoError(t, err)
		other := failpoint.NewFailpoint[error](callbackKey)
		registry.Set(callbackKey, other)

		_, err = faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
		require.EqualError(t, err, "failpoint callback@namespace-id was replaced")

		// Removing the last callback leaves the failpoint that replaced it alone.
		remove()
		require.Same(t, other, registry.Get(callbackKey))
	})
}

func TestAddCallback_RemoveDuringExecution(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	injected := errors.New("injected")
	started := make(chan struct{})
	release := make(chan struct{})
	remove, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error {
		close(started)
		<-release
		return injected
	})
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		err, _ := failpoint.ExecuteWith[error](registrySource{registry: registry}, callbackKey, nil)
		result <- err
	}()
	await.Rcv(t, started)
	remove()
	close(release)
	require.Equal(t, injected, await.Rcv(t, result))
	require.Nil(t, registry.Get(callbackKey))
}

func TestAddCallback_LastRemoveFallsBackToUnscoped(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	unscoped := errors.New("unscoped")
	fp := failpoint.NewFailpoint[error](callbackKey.ForScope(""))
	fp.Return(unscoped, failpoint.Always)
	registry.Set(callbackKey.ForScope(""), fp)
	remove, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
	require.NoError(t, err)

	// The scoped failpoint takes precedence while it has a callback.
	got, _ := failpoint.ExecuteWith[error](src, callbackKey, nil)
	require.NoError(t, got)

	remove()
	got, _ = failpoint.ExecuteWith[error](src, callbackKey, nil)
	require.Equal(t, unscoped, got)
}

func TestAddCallback_OwnedFailpoint(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	remove, err := faultinjectiontest.AddCallback(registry, callbackKey, func(any) error { return nil })
	require.NoError(t, err)
	t.Cleanup(remove)
	fp := registry.Get(callbackKey)
	require.Equal(t, faultinjectiontest.CallbackOwner, fp.(*failpoint.Failpoint[error]).Owner())

	// The FailpointService neither reconfigures nor removes it.
	server := failpointgrpc.NewServer(registry)
	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{Key: callbackKey.Name(), Scope: callbackKey.Scope()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: callbackKey.Name(), Scope: callbackKey.Scope()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = server.DisableAllFailpoints(t.Context(), &failpointpb.DisableAllFailpointsRequest{})
	require.NoError(t, err)
	require.Same(t, fp, registry.Get(callbackKey))
	require.True(t, fp.(*failpoint.Failpoint[error]).Info().Enabled)
}
