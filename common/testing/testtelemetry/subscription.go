package testtelemetry

import (
	"context"
	"sync"
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// subscription represents a test's view of spans from a MemoryExporter.
// Each test has a single shared subscription that accumulates spans.
// The subscription is automatically closed when the test ends.
type subscription struct {
	exporter *MemoryExporter
	t        *testing.T
	mu       sync.Mutex
	cond     *sync.Cond
	buffer   []*tracepb.ResourceSpans
	closed   bool
}

// spans returns all buffered spans and clears the buffer.
func (s *subscription) spans() []*tracepb.ResourceSpans {
	s.mu.Lock()
	defer s.mu.Unlock()
	spans := s.buffer
	s.buffer = nil
	return spans
}

// wait blocks until new spans are available or the context is cancelled.
// Returns nil if spans are available, or the context error if cancelled.
func (s *subscription) wait(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.buffer) > 0 || s.closed {
		return nil
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-done:
		}
	}()

	for len(s.buffer) == 0 && !s.closed && ctx.Err() == nil {
		s.cond.Wait()
	}
	close(done)

	return ctx.Err()
}
