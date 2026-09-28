package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestClientCacheReplacesInvalidEntry(t *testing.T) {
	firstValid := true
	createCalls := 0
	releaseCalls := 0
	cache := NewClientCacheWithEntryProvider(nil, func(string) (ClientCacheEntry, error) {
		createCalls++
		if createCalls == 1 {
			return ClientCacheEntry{
				Client:  "first",
				IsValid: func() bool { return firstValid },
				Release: func() error { releaseCalls++; return nil },
			}, nil
		}
		return ClientCacheEntry{Client: "replacement"}, nil
	}, log.NewNoopLogger())

	client, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Equal(t, "first", client)
	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Equal(t, "first", client)
	require.Equal(t, 1, createCalls)

	firstValid = false
	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Equal(t, "replacement", client)
	require.Equal(t, 2, createCalls)
	require.Equal(t, 1, releaseCalls)

	client, err = cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Equal(t, "replacement", client)
	require.Equal(t, 2, createCalls)
}
