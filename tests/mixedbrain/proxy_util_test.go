package mixedbrain

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFrontendProxyRoundRobin(t *testing.T) {
	p := startFrontendProxy(t)
	t.Cleanup(p.stop)
	require.NoError(t, p.AddBackend("current-0", "current", "127.0.0.1:1"))
	require.NoError(t, p.AddBackend("release-0", "release", "127.0.0.1:2"))

	for range 6 {
		_, _, err := p.director(context.Background(), "")
		require.NoError(t, err)
	}

	current, ok := p.BackendCallCount("current-0")
	require.True(t, ok)
	require.Equal(t, int64(3), current)
	require.Equal(t, int64(3), p.VersionCallCount("release"))
}

func TestFrontendProxyReplacementPreservesCount(t *testing.T) {
	p := startFrontendProxy(t)
	t.Cleanup(p.stop)
	require.NoError(t, p.AddBackend("current-0", "current", "127.0.0.1:1"))
	_, _, err := p.director(context.Background(), "")
	require.NoError(t, err)

	require.NoError(t, p.RemoveBackend("current-0"))
	require.NoError(t, p.AddBackend("current-0", "current", "127.0.0.1:2"))
	_, _, err = p.director(context.Background(), "")
	require.NoError(t, err)

	count, ok := p.BackendCallCount("current-0")
	require.True(t, ok)
	require.Equal(t, int64(2), count)
}

func TestFrontendProxyBackendErrors(t *testing.T) {
	p := startFrontendProxy(t)
	t.Cleanup(p.stop)

	require.ErrorIs(t, p.RemoveBackend("missing"), errBackendNotFound)
	require.NoError(t, p.AddBackend("current-0", "current", "127.0.0.1:1"))
	require.ErrorIs(t, p.AddBackend("current-0", "current", "127.0.0.1:2"), errBackendAlreadyActive)
	require.NoError(t, p.RemoveBackend("current-0"))
	require.ErrorIs(t, p.RemoveBackend("current-0"), errBackendNotFound)
	require.ErrorIs(t, p.AddBackend("current-0", "release", "127.0.0.1:2"), errBackendVersionChanged)
}

func TestFrontendProxyUnavailableWithNoBackends(t *testing.T) {
	p := startFrontendProxy(t)
	t.Cleanup(p.stop)

	_, _, err := p.director(context.Background(), "")
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestFrontendProxyConcurrentAddRemoveAndRoute(t *testing.T) {
	p := startFrontendProxy(t)
	t.Cleanup(p.stop)
	require.NoError(t, p.AddBackend("stable", "current", "127.0.0.1:1"))

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			for range 500 {
				_, _, err := p.director(context.Background(), "")
				if err != nil {
					errs <- fmt.Errorf("route request: %w", err)
					return
				}
			}
		})
	}
	for i := range 100 {
		name := "churn"
		require.NoError(t, p.AddBackend(name, "release", "127.0.0.1:2"))
		_, _, err := p.director(context.Background(), "")
		require.NoError(t, err)
		require.NoError(t, p.RemoveBackend(name), i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	stable, ok := p.BackendCallCount("stable")
	require.True(t, ok)
	require.Positive(t, stable)
	require.Positive(t, p.VersionCallCount("release"))
}
