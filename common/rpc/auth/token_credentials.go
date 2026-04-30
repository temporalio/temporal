package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const defaultGraceWindow = 30 * time.Second

type TokenCredentials struct {
	headerName   string
	fetchToken   func(context.Context) (string, error)
	graceWindow  time.Duration
	requireToken bool

	mu          sync.Mutex
	cachedToken string
	expiry      time.Time // zero value means no expiry (token has no exp claim)
}

var _ credentials.PerRPCCredentials = (*TokenCredentials)(nil)

// NewTokenCredentials returns a PerRPCCredentials that attaches a Bearer token from fetchToken.
// When requireToken is true an empty fetched token returns codes.Unauthenticated rather than sending the RPC without a header.
func NewTokenCredentials(
	headerName string,
	fetchToken func(context.Context) (string, error),
	graceWindow time.Duration,
	requireToken bool,
) *TokenCredentials {
	if graceWindow == 0 {
		graceWindow = defaultGraceWindow
	}
	return &TokenCredentials{
		headerName:   headerName,
		fetchToken:   fetchToken,
		graceWindow:  graceWindow,
		requireToken: requireToken,
	}
}

func (c *TokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.getToken(ctx)
	if err != nil {
		return nil, err
	}
	if token == "" {
		if c.requireToken {
			return nil, status.Error(codes.Unauthenticated, "no auth token available for outbound remote-cluster RPC")
		}
		return nil, nil
	}
	return map[string]string{
		c.headerName: "Bearer " + token,
	}, nil
}

func (c *TokenCredentials) RequireTransportSecurity() bool {
	return false
}

func (c *TokenCredentials) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cachedToken != "" && !c.isExpired() {
		return c.cachedToken, nil
	}

	token, err := c.fetchToken(ctx)
	if err != nil {
		if c.cachedToken != "" && !c.isHardExpired() {
			return c.cachedToken, nil
		}
		return "", fmt.Errorf("failed to fetch auth token: %w", err)
	}

	c.cachedToken = token
	c.expiry = parseJWTExpiry(token)
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

// parseJWTExpiry extracts the exp claim from a JWT without verifying the signature.
// Returns zero time if the token has no exp claim or cannot be parsed.
func parseJWTExpiry(token string) time.Time {
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

	return time.Unix(int64(*claims.Exp), 0)
}
