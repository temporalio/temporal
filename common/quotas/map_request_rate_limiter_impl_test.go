package quotas

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	mapRequestRateLimiterSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestMapRequestRateLimiterSuite(t *testing.T) {
	s := new(mapRequestRateLimiterSuite)
	suite.Run(t, s)
}

func (s *mapRequestRateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *mapRequestRateLimiterSuite) TestGetOrCreateRateLimiter() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// First access creates a new rate limiter
	rateLimiter.Allow(now, req1)
	s.Equal(1, createCount)

	// Same key reuses existing rate limiter
	rateLimiter.Allow(now, req1)
	s.Equal(1, createCount)

	// Different key creates new rate limiter
	rateLimiter.Allow(now, req2)
	s.Equal(2, createCount)
}

func (s *mapRequestRateLimiterSuite) TestCleanup() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttlNano = int64(100 * time.Millisecond)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// Create entries for both namespaces
	rateLimiter.Allow(now, req1)
	rateLimiter.Allow(now.Add(10*time.Millisecond), req2)
	s.Equal(2, createCount)

	// Verify both entries exist
	s.Len(rateLimiter.rateLimiters, 2)

	// Trigger cleanup after TTL expires for both
	rateLimiter.cleanup(now.Add(200 * time.Millisecond))

	// Both should be evicted
	s.Empty(rateLimiter.rateLimiters)
}

func (s *mapRequestRateLimiterSuite) TestAccessRefreshesTTL() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttlNano = int64(100 * time.Millisecond)

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// Create entries for both namespaces
	rateLimiter.Allow(now, req1)
	rateLimiter.Allow(now.Add(10*time.Millisecond), req2)
	s.Equal(2, createCount)

	// Access namespace1 again to refresh its TTL
	rateLimiter.Allow(now.Add(50*time.Millisecond), req1)
	s.Equal(2, createCount) // No new rate limiter created

	// Cleanup at now+150ms should evict namespace2 (>100ms since last access at now+10ms)
	// but namespace1 should survive (last accessed at now+50ms, only 100ms ago)
	rateLimiter.cleanup(now.Add(150 * time.Millisecond))

	s.Len(rateLimiter.rateLimiters, 1)
	_, exists := rateLimiter.rateLimiters["namespace1"]
	s.True(exists)
	_, exists = rateLimiter.rateLimiters["namespace2"]
	s.False(exists)
}

func (s *mapRequestRateLimiterSuite) TestNamespaceRequestRateLimiter() {
	createCount := 0
	rateLimiter := NewNamespaceRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
	)

	now := time.Now()

	// Uses Caller as key
	rateLimiter.Allow(now, Request{Caller: "namespace1"})
	s.Equal(1, createCount)

	rateLimiter.Allow(now, Request{Caller: "namespace1"})
	s.Equal(1, createCount)

	rateLimiter.Allow(now, Request{Caller: "namespace2"})
	s.Equal(2, createCount)
}
