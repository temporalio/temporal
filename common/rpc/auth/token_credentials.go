package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/credentials"
)

const defaultGraceWindow = 30 * time.Second

type TokenCredentials struct {
	headerName  string
	fetchToken  func(context.Context) (string, time.Time, error)
	graceWindow time.Duration

	mu          sync.Mutex
	cachedToken string
	expiry      time.Time // zero value means no expiry (provider didn't supply one)
	sf          singleflight.Group
}

var _ credentials.PerRPCCredentials = (*TokenCredentials)(nil)

func NewTokenCredentials(
	headerName string,
	fetchToken func(context.Context) (string, time.Time, error),
	graceWindow time.Duration,
) *TokenCredentials {
	if graceWindow == 0 {
		graceWindow = defaultGraceWindow
	}
	return &TokenCredentials{
		headerName:  headerName,
		fetchToken:  fetchToken,
		graceWindow: graceWindow,
	}
}

func (c *TokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.getToken(ctx)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, nil
	}
	return map[string]string{
		c.headerName: "Bearer " + token,
	}, nil
}

// RequireTransportSecurity returns true so the bearer never attaches to plaintext dials, per RFC 9700.
func (c *TokenCredentials) RequireTransportSecurity() bool {
	return true
}

func (c *TokenCredentials) getToken(ctx context.Context) (string, error) {
	// Fast path: cache hit. Hold the lock only long enough to read.
	c.mu.Lock()
	if c.cachedToken != "" && !c.isExpired() {
		token := c.cachedToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()

	// Slow path: coalesce concurrent fetches so a slow IdP round-trip doesn't
	// serialize every RPC on the connection. The cache mutex is never held
	// while c.fetchToken is in flight.
	v, err, _ := c.sf.Do("token", func() (any, error) {
		// Re-check under lock — another fetcher may have just updated cache.
		c.mu.Lock()
		if c.cachedToken != "" && !c.isExpired() {
			cached := c.cachedToken
			c.mu.Unlock()
			return cached, nil
		}
		c.mu.Unlock()

		token, expiresAt, fetchErr := c.fetchToken(ctx)
		if fetchErr != nil {
			c.mu.Lock()
			if c.cachedToken != "" && !c.isHardExpired() {
				cached := c.cachedToken
				c.mu.Unlock()
				return cached, nil
			}
			c.mu.Unlock()
			return nil, fmt.Errorf("failed to fetch auth token: %w", fetchErr)
		}

		c.mu.Lock()
		c.cachedToken = token
		c.expiry = expiresAt
		c.mu.Unlock()
		return token, nil
	})
	if err != nil {
		return "", err
	}
	token, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("token cache returned unexpected type %T", v)
	}
	return token, nil
}

// isExpired returns true if the token is within the grace window of expiry.
func (c *TokenCredentials) isExpired() bool {
	if c.expiry.IsZero() {
		return false
	}
	return time.Now().After(c.expiry.Add(-c.graceWindow))
}

// isHardExpired returns true if the token is past its expiry time (no grace).
func (c *TokenCredentials) isHardExpired() bool {
	if c.expiry.IsZero() {
		return false
	}
	return time.Now().After(c.expiry)
}

// ParseJWTExpiry extracts the exp claim from a JWT without verifying the signature.
// Returns zero time if the token has no exp claim or cannot be parsed.
// Exposed as a helper for JWT-based TokenProvider implementations to compute the
// expiresAt value they return from GetToken.
func ParseJWTExpiry(token string) time.Time {
	parts := strings.SplitN(token, ".", 3)
	if len(parts) < 2 {
		return time.Time{}
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}

	var claims struct {
		Exp *float64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return time.Time{}
	}

	// RFC 7519 allows fractional NumericDate; sub-second precision is irrelevant for cache windows so the int64 truncation is intentional.
	return time.Unix(int64(*claims.Exp), 0)
}
