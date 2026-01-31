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

func (s *mapRequestRateLimiterSuite) TestLazyEviction() {
	createCount := 0
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			createCount++
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttl = 100 * time.Millisecond

	now := time.Now()
	req1 := Request{Caller: "namespace1"}
	req2 := Request{Caller: "namespace2"}

	// Create entry for namespace1
	rateLimiter.Allow(now, req1)
	s.Equal(1, createCount)

	// Verify namespace1 entry exists
	s.Equal(1, len(rateLimiter.byKey))

	// Access with namespace2 after TTL expires triggers eviction of namespace1
	futureTime := now.Add(200 * time.Millisecond)
	rateLimiter.Allow(futureTime, req2)
	s.Equal(2, createCount)

	// namespace1 should be evicted, only namespace2 remains
	s.Equal(1, len(rateLimiter.byKey))
	_, exists := rateLimiter.byKey["namespace1"]
	s.False(exists)
	_, exists = rateLimiter.byKey["namespace2"]
	s.True(exists)
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
	rateLimiter.ttl = 100 * time.Millisecond

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

	// Now namespace2 is the oldest (last accessed at now+10ms)
	// Access at now+150ms should evict namespace2 (>100ms since last access)
	// but namespace1 should survive (last accessed at now+50ms, only 100ms ago)
	rateLimiter.Allow(now.Add(150*time.Millisecond), req1)

	s.Equal(1, len(rateLimiter.byKey))
	_, exists := rateLimiter.byKey["namespace1"]
	s.True(exists)
	_, exists = rateLimiter.byKey["namespace2"]
	s.False(exists)
}

func (s *mapRequestRateLimiterSuite) TestEvictsOnlyOneEntryPerAccess() {
	rateLimiter := NewMapRequestRateLimiter(
		func(req Request) RequestRateLimiter {
			return NoopRequestRateLimiter
		},
		func(req Request) string { return req.Caller },
	)
	// Override TTL for testing
	rateLimiter.ttl = 100 * time.Millisecond

	now := time.Now()

	// Create 3 entries
	rateLimiter.Allow(now, Request{Caller: "ns1"})
	rateLimiter.Allow(now.Add(10*time.Millisecond), Request{Caller: "ns2"})
	rateLimiter.Allow(now.Add(20*time.Millisecond), Request{Caller: "ns3"})
	s.Equal(3, len(rateLimiter.byKey))

	// Access after all entries are expired
	// Should only evict one entry (the oldest: ns1)
	rateLimiter.Allow(now.Add(200*time.Millisecond), Request{Caller: "ns4"})

	// ns1 evicted, ns4 added = still have ns2, ns3, ns4
	s.Equal(3, len(rateLimiter.byKey))
	_, exists := rateLimiter.byKey["ns1"]
	s.False(exists, "ns1 should be evicted")
	_, exists = rateLimiter.byKey["ns4"]
	s.True(exists, "ns4 should exist")
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
