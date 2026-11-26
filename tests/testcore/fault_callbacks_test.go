package testcore

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/testx/failpoint"
)

var faultCallbackKey = failpoint.NewKey[error]("testcore.faultCallback").ForScope("namespace-id")

func TestFaultCallbacks_RunInOrderUntilError(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	injected := errors.New("injected")
	var calls []string
	add := func(name string, err error) func() {
		remove, addErr := callbacks.add(faultCallbackKey, func(arg any) error {
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

	result := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{Arg: "arg"}, faultCallbackKey)
	require.True(t, result.Fired)
	require.Equal(t, injected, result.Value)
	require.Equal(t, []string{"first", "second"}, calls)

	calls = nil
	removeSecond()
	failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{Arg: "arg"}, faultCallbackKey)
	require.Equal(t, []string{"first", "third"}, calls)
}

func TestFaultCallbacks_LastRemoveRemovesFailpoint(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	removeFirst, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
	require.NoError(t, err)
	removeSecond, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
	require.NoError(t, err)
	fp := registry.Get(faultCallbackKey)
	require.NotNil(t, fp)

	removeFirst()
	removeFirst()
	require.Same(t, fp, registry.Get(faultCallbackKey))

	removeSecond()
	require.Nil(t, registry.Get(faultCallbackKey))
	_, set := fp.(*failpoint.Failpoint[error]).Execute()
	require.False(t, set, "the removed failpoint is disabled")

	// A new callback registers a new failpoint.
	removeThird, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
	require.NoError(t, err)
	defer removeThird()
	require.NotNil(t, registry.Get(faultCallbackKey))
	require.NotSame(t, fp, registry.Get(faultCallbackKey))
}

func TestFaultCallbacks_KeysAreSeparate(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	otherScope := faultCallbackKey.ForScope("other")
	for _, key := range []failpoint.Key[error]{faultCallbackKey, otherScope} {
		remove, err := callbacks.add(key, func(any) error { return nil })
		require.NoError(t, err)
		t.Cleanup(remove)
	}
	require.NotSame(t, registry.Get(faultCallbackKey), registry.Get(otherScope))
}

func TestFaultCallbacks_OtherFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("registered before", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		callbacks := newFaultCallbacks(registry)
		other := failpoint.NewFailpoint(faultCallbackKey)
		registry.Set(other)

		_, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
		require.EqualError(t, err, "failpoint testcore.faultCallback@namespace-id is already registered")
		require.Same(t, other, registry.Get(faultCallbackKey))
	})

	t.Run("replaced", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		callbacks := newFaultCallbacks(registry)
		remove, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
		require.NoError(t, err)
		other := failpoint.NewFailpoint(faultCallbackKey)
		registry.Set(other)

		_, err = callbacks.add(faultCallbackKey, func(any) error { return nil })
		require.EqualError(t, err, "failpoint testcore.faultCallback@namespace-id was replaced")

		// Removing the last callback leaves the failpoint that replaced it alone.
		remove()
		require.Same(t, other, registry.Get(faultCallbackKey))
	})
}

func TestFaultCallbacks_RemoveDuringExecution(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	injected := errors.New("injected")
	started := make(chan struct{})
	release := make(chan struct{})
	remove, err := callbacks.add(faultCallbackKey, func(any) error {
		close(started)
		<-release
		return injected
	})
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		result <- failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, faultCallbackKey).Value
	}()
	await.Rcv(t, started)
	remove()
	close(release)
	require.Equal(t, injected, await.Rcv(t, result))
	require.Nil(t, registry.Get(faultCallbackKey))
}

func TestFaultCallbacks_LastRemoveFallsBackToUnscoped(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	unscoped := errors.New("unscoped")
	fp := failpoint.NewFailpoint(faultCallbackKey.ForScope(""))
	fp.Return(unscoped)
	registry.Set(fp)
	remove, err := callbacks.add(faultCallbackKey, func(any) error { return nil })
	require.NoError(t, err)

	// The scoped failpoint takes precedence while it has a callback.
	require.NoError(t, failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, faultCallbackKey).Value)

	remove()
	require.Equal(t, unscoped, failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{}, faultCallbackKey).Value)
}

// TestFaultCallbacks_Concurrent executes the failpoint while callbacks are added and removed. Run
// it with -race.
func TestFaultCallbacks_Concurrent(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	injected := errors.New("injected")
	stop := make(chan struct{})

	var executors sync.WaitGroup
	for range 4 {
		executors.Add(1)
		go func() {
			defer executors.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{Arg: "arg"}, faultCallbackKey).Value; err != nil {
					if !errors.Is(err, injected) {
						t.Errorf("unexpected error %v", err)
					}
				}
			}
		}()
	}

	var adders sync.WaitGroup
	for range 4 {
		adders.Add(1)
		go func() {
			defer adders.Done()
			for range 200 {
				remove, err := callbacks.add(faultCallbackKey, func(any) error { return injected })
				if err != nil {
					t.Errorf("add: %v", err)
					return
				}
				remove()
			}
		}()
	}
	adders.Wait()
	close(stop)
	executors.Wait()

	require.Nil(t, registry.Get(faultCallbackKey), "the last removal removed the failpoint")
}
