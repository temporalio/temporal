package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func makeTestJWT(exp *int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := map[string]any{"sub": "test"}
	if exp != nil {
		claims["exp"] = *exp
	}
	payload, _ := json.Marshal(claims)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payload)
	return header + "." + payloadB64 + ".fakesig"
}

func TestTokenCredentials_FetchesOnFirstCall(t *testing.T) {
	t.Parallel()
	expiresAt := time.Now().Add(time.Hour)
	token := makeTestJWT(ptr(expiresAt.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		callCount.Add(1)
		return token, expiresAt, nil
	}, time.Second)

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+token, md["authorization"])
	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_UsesCachedToken(t *testing.T) {
	t.Parallel()
	expiresAt := time.Now().Add(time.Hour)
	token := makeTestJWT(ptr(expiresAt.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		callCount.Add(1)
		return token, expiresAt, nil
	}, time.Second)

	_, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	_, err = creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	_, err = creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_RefreshesExpiredToken(t *testing.T) {
	t.Parallel()
	oldExp := time.Now().Add(-time.Minute)
	newExp := time.Now().Add(time.Hour)
	oldToken := makeTestJWT(ptr(oldExp.Unix()))
	newToken := makeTestJWT(ptr(newExp.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		if callCount.Add(1) == 1 {
			return oldToken, oldExp, nil
		}
		return newToken, newExp, nil
	}, time.Second)

	// First call fetches the expired token, which triggers an immediate re-fetch on second call.
	md1, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+oldToken, md1["authorization"])

	// Second call sees the token is expired and re-fetches.
	md2, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+newToken, md2["authorization"])
	require.Equal(t, int32(2), callCount.Load())
}

func TestTokenCredentials_RefreshesWithinGraceWindow(t *testing.T) {
	t.Parallel()
	// Token expires in 5 seconds, grace window is 10 seconds — should trigger refresh.
	oldExp := time.Now().Add(5 * time.Second)
	newExp := time.Now().Add(time.Hour)
	oldToken := makeTestJWT(ptr(oldExp.Unix()))
	newToken := makeTestJWT(ptr(newExp.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		if callCount.Add(1) == 1 {
			return oldToken, oldExp, nil
		}
		return newToken, newExp, nil
	}, 10*time.Second)

	_, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	// Second call: within grace window, should refresh.
	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+newToken, md["authorization"])
}

func TestTokenCredentials_FallsBackOnFetchError(t *testing.T) {
	t.Parallel()
	// Token is within grace window but not hard-expired — fallback to cached.
	expSoon := time.Now().Add(5 * time.Second)
	token := makeTestJWT(ptr(expSoon.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		if callCount.Add(1) == 1 {
			return token, expSoon, nil
		}
		return "", time.Time{}, errors.New("token server unavailable")
	}, 10*time.Second)

	_, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	// Fetch fails, but token is not hard-expired — use cached.
	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+token, md["authorization"])
}

func TestTokenCredentials_ErrorsWhenHardExpiredAndFetchFails(t *testing.T) {
	t.Parallel()
	expPast := time.Now().Add(-time.Minute)
	token := makeTestJWT(ptr(expPast.Unix()))
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		if callCount.Add(1) == 1 {
			return token, expPast, nil
		}
		return "", time.Time{}, errors.New("token server unavailable")
	}, time.Second)

	_, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	// Token is hard-expired and fetch fails — error.
	_, err = creds.GetRequestMetadata(context.Background())
	require.ErrorContains(t, err, "failed to fetch auth token")
}

func TestTokenCredentials_NoExpClaimNeverExpires(t *testing.T) {
	t.Parallel()
	token := makeTestJWT(nil)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		callCount.Add(1)
		return token, time.Time{}, nil
	}, time.Second)

	for range 5 {
		_, err := creds.GetRequestMetadata(context.Background())
		require.NoError(t, err)
	}

	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_EmptyTokenReturnsNilMetadata(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		return "", time.Time{}, nil
	}, time.Second)

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Nil(t, md)
}

func TestTokenCredentials_RequireTransportSecurity(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, time.Time, error) {
		return "token", time.Time{}, nil
	}, 0)
	require.True(t, creds.RequireTransportSecurity(), "production credentials must require TLS")
}

func TestParseJWTExpiry_Valid(t *testing.T) {
	t.Parallel()
	exp := int64(1700000000)
	token := makeTestJWT(&exp)
	result := ParseJWTExpiry(token)
	require.Equal(t, time.Unix(1700000000, 0), result)
}

func TestParseJWTExpiry_NoExp(t *testing.T) {
	t.Parallel()
	token := makeTestJWT(nil)
	result := ParseJWTExpiry(token)
	require.True(t, result.IsZero())
}

func TestParseJWTExpiry_InvalidToken(t *testing.T) {
	t.Parallel()
	require.True(t, ParseJWTExpiry("not-a-jwt").IsZero())
	require.True(t, ParseJWTExpiry("").IsZero())
	require.True(t, ParseJWTExpiry("a.!!!invalid-base64.c").IsZero())
}

func ptr[T any](v T) *T { return &v }
