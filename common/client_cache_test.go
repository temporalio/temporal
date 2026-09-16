package common

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

type clientCacheTestResolver struct{}

func (clientCacheTestResolver) Lookup(key string, _ int) (string, error) {
	return key, nil
}

func (clientCacheTestResolver) GetAllAddresses() ([]string, error) {
	return nil, nil
}

func TestClientCacheWithProviderReplacesInvalidEntryOnce(t *testing.T) {
	var calls atomic.Int32
	var releases atomic.Int32
	var firstValid atomic.Bool
	firstValid.Store(true)
	cache := NewClientCacheWithProvider(
		clientCacheTestResolver{},
		func(string) (ClientCacheEntry, error) {
			if calls.Add(1) == 1 {
				return ClientCacheEntry{
					Client:  "first",
					IsValid: firstValid.Load,
					Release: func() error { releases.Add(1); return nil },
				}, nil
			}
			return ClientCacheEntry{Client: "replacement"}, nil
		},
		log.NewNoopLogger(),
	)

	client, err := cache.GetClientForClientKey("key")
	require.NoError(t, err)
	require.Equal(t, "first", client)
	firstValid.Store(false)

	const goroutines = 32
	type result struct {
		client any
		err    error
	}
	results := make(chan result, goroutines)
	var waitGroup sync.WaitGroup
	waitGroup.Add(goroutines)
	for range goroutines {
		go func() {
			defer waitGroup.Done()
			client, err := cache.GetClientForClientKey("key")
			results <- result{client: client, err: err}
		}()
	}
	waitGroup.Wait()
	close(results)

	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, "replacement", result.client)
	}
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), releases.Load())
}

func TestClientCacheWithProviderFailedReplacementIsRetryable(t *testing.T) {
	replacementErr := errors.New("replacement failed")
	var calls atomic.Int32
	var releases atomic.Int32
	var valid atomic.Bool
	valid.Store(true)
	cache := NewClientCacheWithProvider(
		clientCacheTestResolver{},
		func(string) (ClientCacheEntry, error) {
			switch calls.Add(1) {
			case 1:
				return ClientCacheEntry{
					Client:  "first",
					IsValid: valid.Load,
					Release: func() error { releases.Add(1); return nil },
				}, nil
			case 2:
				return ClientCacheEntry{}, replacementErr
			default:
				return ClientCacheEntry{Client: "replacement"}, nil
			}
		},
		log.NewNoopLogger(),
	)

	_, err := cache.GetClientForClientKey("key")
	require.NoError(t, err)
	valid.Store(false)

	_, err = cache.GetClientForClientKey("key")
	require.ErrorIs(t, err, replacementErr)
	require.Zero(t, releases.Load())

	client, err := cache.GetClientForClientKey("key")
	require.NoError(t, err)
	require.Equal(t, "replacement", client)
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, int32(3), calls.Load())
}

func TestNewClientCacheKeepsEntriesWithoutValidator(t *testing.T) {
	var calls atomic.Int32
	cache := NewClientCache(
		clientCacheTestResolver{},
		func(string) (any, func() error, error) {
			calls.Add(1)
			return "client", nil, nil
		},
		log.NewNoopLogger(),
	)

	for range 2 {
		client, err := cache.GetClientForClientKey("key")
		require.NoError(t, err)
		require.Equal(t, "client", client)
	}
	require.Equal(t, int32(1), calls.Load())
}
