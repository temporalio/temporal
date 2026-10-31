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
	sharedcontext "go.temporal.io/testx/testcontext"
)

type forwardingSuite struct {
	parallelsuite.Suite[*forwardingSuite]
}

func (s *forwardingSuite) TestAssertions() {
	s.ProtoEqual(&historypb.History{}, &historypb.History{})
	s.EqualHistoryEvents("", nil)

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
	s.Same(testcontext.For(s.T()), sharedcontext.For(s.T()))

	s.Await(func(s *forwardingSuite) {
		s.Equal("decorated", s.Context().Value(key{}))
	}, time.Second, time.Millisecond)
}

func TestForwarding(t *testing.T) {
	require.NotPanics(t, func() { parallelsuite.Run(t, &forwardingSuite{}) })
}
