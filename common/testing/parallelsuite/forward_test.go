package parallelsuite_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
)

type forwardingSuite struct {
	parallelsuite.Suite[*forwardingSuite]
}

func (s *forwardingSuite) TestAssertions() {
	s.ProtoEqual(&historypb.History{}, &historypb.History{})
	s.EqualHistoryEvents("", nil)
	ch := make(chan int, 1)
	s.Snd(ch, 42)
	s.Equal(42, s.Rcv(ch))

	var attempts atomic.Int32
	s.Await(func(s *forwardingSuite) {
		s.ProtoEqual(&historypb.HistoryEvent{EventId: 2}, &historypb.HistoryEvent{EventId: int64(attempts.Add(1))})
		s.EqualHistoryEvents("", nil)
	}, time.Second, time.Millisecond)
	s.Equal(int32(2), attempts.Load())

	s.Run("subtest", func(s *forwardingSuite) {
		s.ProtoEqual(&historypb.History{}, &historypb.History{})
		s.EqualHistoryEvents("", nil)
	})
}

func (s *forwardingSuite) TestContext() {
	type key struct{}
	testcontext.AttachDecorator(s.T(), key{}, func(ctx context.Context) context.Context {
		return context.WithValue(ctx, key{}, "decorated")
	})
	s.Equal("decorated", s.Context().Value(key{}))

	s.Await(func(s *forwardingSuite) {
		s.Equal("decorated", s.Context().Value(key{}))
	}, time.Second, time.Millisecond)
}

func TestForwarding(t *testing.T) {
	require.NotPanics(t, func() { parallelsuite.Run(t, &forwardingSuite{}) })
}

type awaitTrueSuite struct {
	parallelsuite.Suite[*awaitTrueSuite]
}

func (s *awaitTrueSuite) TestAwaitTrue() {
	var attempts atomic.Int32
	s.AwaitTrue(func() bool {
		attempts.Add(1)
		return true
	}, time.Second, time.Millisecond)
	s.Equal(int32(1), attempts.Load())
}

func (s *awaitTrueSuite) TestAwaitTrueFalseRetry() {
	var attempts atomic.Int32
	s.AwaitTrue(func() bool {
		return attempts.Add(1) == 2
	}, time.Second, time.Millisecond)
	s.Equal(int32(2), attempts.Load())
}

func (s *awaitTrueSuite) TestAwaitTruef() {
	s.AwaitTruef(func() bool {
		return true
	}, time.Second, time.Millisecond, "condition should pass")
}

type contextSuite struct {
	parallelsuite.Suite[*contextSuite]
}

func (s *contextSuite) TestContextHasDeadline() {
	deadline, ok := s.Context().Deadline()
	s.True(ok)
	s.Positive(time.Until(deadline))
}

func (s *contextSuite) TestAwaitUsesSuiteContext() {
	type key struct{}

	testcontext.AttachDecorator(s.T(), key{}, func(ctx context.Context) context.Context {
		return context.WithValue(ctx, key{}, "decorated")
	})

	s.Await(func(s *contextSuite) {
		s.Equal("decorated", s.Context().Value(key{}))
		deadline, ok := s.Context().Deadline()
		s.True(ok)
		s.Less(time.Until(deadline), 200*time.Millisecond)
	}, 100*time.Millisecond, time.Millisecond)
}

func TestServerHelpers(t *testing.T) {
	t.Run("await true", func(t *testing.T) { parallelsuite.Run(t, &awaitTrueSuite{}) })
	t.Run("context", func(t *testing.T) { parallelsuite.Run(t, &contextSuite{}) })
}

type serverGuardSuite struct {
	parallelsuite.Suite[*serverGuardSuite]
}

func (s *serverGuardSuite) TestGuard() {
	t := s.T()
	s.Run("subtest", func(s *serverGuardSuite) { s.NotNil(s.T()) })
	// Context() resolves through T() and also panics after Run.
	require.Panics(t, func() { s.Context() })

	// Channel helpers resolve through T() and also panic after Run.
	ch := make(chan int, 1)
	require.Panics(t, func() { s.Snd(ch, 42) })
	require.Panics(t, func() { s.Rcv(ch) })
}

func TestServerGuard(t *testing.T) { parallelsuite.Run(t, &serverGuardSuite{}) }
