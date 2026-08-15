package umpiretest

import (
	"sync"
	"testing"

	"go.temporal.io/server/tests/umpire2"
)

var (
	canonicalProtocolOnce sync.Once
	canonicalProtocol     *umpire2.Protocol
	canonicalProtocolErr  error
)

// CanonicalProtocol returns the process-cached immutable Temporal protocol.
func CanonicalProtocol() (*umpire2.Protocol, error) {
	canonicalProtocolOnce.Do(func() {
		canonicalProtocol, canonicalProtocolErr = umpire2.DefaultProtocol()
	})
	return canonicalProtocol, canonicalProtocolErr
}

// RequireProtocol returns the canonical protocol or fails an author-facing test immediately.
func RequireProtocol(t testing.TB) *umpire2.Protocol {
	t.Helper()
	protocol, err := CanonicalProtocol()
	if err != nil {
		t.Fatalf("umpiretest: compile canonical protocol: %v", err)
	}
	return protocol
}
