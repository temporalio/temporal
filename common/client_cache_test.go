package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestClientCacheReplacesInvalidEntry(t *testing.T) {
	first := new(int)
	second := new(int)
	firstValid := true
	createCalls := 0
	firstReleases := 0
	secondReleases := 0
	cache := NewClientCacheWithEntryProvider(nil, func(string) (ClientCacheEntry, error) {
		createCalls++
		if createCalls == 1 {
			return ClientCacheEntry{
				Client:  first,
				IsValid: func() bool { return firstValid },
				Release: func() error { firstReleases++; return nil },
			}, nil
		}
		return ClientCacheEntry{
			Client:  second,
			IsValid: func() bool { return true },
			Release: func() error { secondReleases++; return nil },
		}, nil
	}, log.NewNoopLogger())

	client, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, first, client)
	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, first, client)
	require.Equal(t, 1, createCalls)

	firstValid = false
	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, second, client)
	require.Equal(t, 2, createCalls)
	require.Equal(t, 1, firstReleases)

	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, second, client)
	require.Equal(t, 2, createCalls)

	cache.EvictAll()
	require.Equal(t, 1, firstReleases)
	require.Equal(t, 1, secondReleases)
}

func TestClientCacheKeepsInvalidEntryWhenReplacementFails(t *testing.T) {
	first := new(int)
	second := new(int)
	firstValid := true
	createCalls := 0
	firstReleases := 0
	providerErr := errors.New("replacement failed")
	cache := NewClientCacheWithEntryProvider(nil, func(string) (ClientCacheEntry, error) {
		createCalls++
		switch createCalls {
		case 1:
			return ClientCacheEntry{
				Client:  first,
				IsValid: func() bool { return firstValid },
				Release: func() error { firstReleases++; return nil },
			}, nil
		case 2:
			return ClientCacheEntry{}, providerErr
		default:
			return ClientCacheEntry{Client: second}, nil
		}
	}, log.NewNoopLogger())

	client, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, first, client)

	firstValid = false
	client, err = cache.GetClientForClientKey("matching:7235")
	require.Nil(t, client)
	require.ErrorIs(t, err, providerErr)
	require.Zero(t, firstReleases)

	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, second, client)
	require.Equal(t, 3, createCalls)
	require.Equal(t, 1, firstReleases)
}
