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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	exp := time.Now().Add(time.Hour).Unix()
	token := makeTestJWT(&exp)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		callCount.Add(1)
		return token, nil
	}, time.Second, false)

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+token, md["authorization"])
	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_UsesCachedToken(t *testing.T) {
	t.Parallel()
	exp := time.Now().Add(time.Hour).Unix()
	token := makeTestJWT(&exp)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		callCount.Add(1)
		return token, nil
	}, time.Second, false)

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
	expPast := time.Now().Add(-time.Minute).Unix()
	expFuture := time.Now().Add(time.Hour).Unix()
	oldToken := makeTestJWT(&expPast)
	newToken := makeTestJWT(&expFuture)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		if callCount.Add(1) == 1 {
			return oldToken, nil
		}
		return newToken, nil
	}, time.Second, false)

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
	expSoon := time.Now().Add(5 * time.Second).Unix()
	expFuture := time.Now().Add(time.Hour).Unix()
	oldToken := makeTestJWT(&expSoon)
	newToken := makeTestJWT(&expFuture)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		if callCount.Add(1) == 1 {
			return oldToken, nil
		}
		return newToken, nil
	}, 10*time.Second, false)

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
	expSoon := time.Now().Add(5 * time.Second).Unix()
	token := makeTestJWT(&expSoon)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		if callCount.Add(1) == 1 {
			return token, nil
		}
		return "", errors.New("token server unavailable")
	}, 10*time.Second, false)

	_, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	// Fetch fails, but token is not hard-expired — use cached.
	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer "+token, md["authorization"])
}

func TestTokenCredentials_ErrorsWhenHardExpiredAndFetchFails(t *testing.T) {
	t.Parallel()
	expPast := time.Now().Add(-time.Minute).Unix()
	token := makeTestJWT(&expPast)
	callCount := atomic.Int32{}

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		if callCount.Add(1) == 1 {
			return token, nil
		}
		return "", errors.New("token server unavailable")
	}, time.Second, false)

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

	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		callCount.Add(1)
		return token, nil
	}, time.Second, false)

	for range 5 {
		_, err := creds.GetRequestMetadata(context.Background())
		require.NoError(t, err)
	}

	require.Equal(t, int32(1), callCount.Load())
}

func TestTokenCredentials_EmptyTokenReturnsNilMetadata(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "", nil
	}, time.Second, false)

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Nil(t, md)
}

func TestTokenCredentials_RequireTokenErrorsOnEmpty(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "", nil
	}, time.Second, true)

	md, err := creds.GetRequestMetadata(context.Background())
	require.Nil(t, md)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

func TestTokenCredentials_RequireTransportSecurity(t *testing.T) {
	t.Parallel()
	creds := NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "token", nil
	}, 0, false)
	require.False(t, creds.RequireTransportSecurity())
}

func TestParseJWTExpiry_Valid(t *testing.T) {
	t.Parallel()
	exp := int64(1700000000)
	token := makeTestJWT(&exp)
	result := parseJWTExpiry(token)
	require.Equal(t, time.Unix(1700000000, 0), result)
}

func TestParseJWTExpiry_NoExp(t *testing.T) {
	t.Parallel()
	token := makeTestJWT(nil)
	result := parseJWTExpiry(token)
	require.True(t, result.IsZero())
}

func TestParseJWTExpiry_InvalidToken(t *testing.T) {
	t.Parallel()
	require.True(t, parseJWTExpiry("not-a-jwt").IsZero())
	require.True(t, parseJWTExpiry("").IsZero())
	require.True(t, parseJWTExpiry("a.!!!invalid-base64.c").IsZero())
}
