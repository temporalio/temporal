package auth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenCredentials_AttachesBearerHeader(t *testing.T) {
	t.Parallel()
	callCount := atomic.Int32{}
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		callCount.Add(1)
		return "the-token", nil
	})

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer the-token", md["authorization"])
	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_CallsFetchEveryTime(t *testing.T) {
	t.Parallel()
	// TokenCredentials does no caching — the provider owns the lifecycle.
	callCount := atomic.Int32{}
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		callCount.Add(1)
		return "the-token", nil
	})

	for range 5 {
		_, err := creds.GetRequestMetadata(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, int32(5), callCount.Load())
}

func TestTokenCredentials_PropagatesFetchError(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "", errors.New("token server unavailable")
	})

	_, err := creds.GetRequestMetadata(context.Background())
	require.ErrorContains(t, err, "token server unavailable")
}

func TestTokenCredentials_EmptyTokenReturnsNilMetadata(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "", nil
	})

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Nil(t, md)
}

func TestTokenCredentials_RequireTransportSecurity(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "token", nil
	})
	require.True(t, creds.RequireTransportSecurity(), "production credentials must require TLS")
}
