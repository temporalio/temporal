package parallelsuite

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

type holdsSuite struct{ Suite[*holdsSuite] }

func (s *holdsSuite) TestAwaitHolds() {
	type key struct{}
	testcontext.AttachDecorator(s.T(), key{}, func(ctx context.Context) context.Context {
		return context.WithValue(ctx, key{}, "decorated")
	})
	start := time.Now()
	attempts := 0
	var attemptCtx context.Context
	s.AwaitHolds(func(c *holdsSuite) {
		attempts++
		attemptCtx = c.Context()
		c.NotSame(s, c)
		c.Equal("decorated", c.Context().Value(key{}))
		c.NotSame(s.AssertionT(), c.AssertionT())
		c.NoError(c.Context().Err())
	}, time.Second, 400*time.Millisecond)
	s.Equal(3, attempts)
	s.Equal(time.Second, time.Since(start))
	s.ErrorIs(attemptCtx.Err(), context.Canceled)
}

func (s *holdsSuite) TestAwaitHoldsf() {
	start := time.Now()
	s.AwaitHoldsf(func(c *holdsSuite) {
		c.NoError(c.Context().Err())
	}, time.Second, time.Second, "activity %s", "scheduled")
	s.Equal(time.Second, time.Since(start))
}

func (s *holdsSuite) TestAwaitHoldsTrue() {
	start := time.Now()
	attempts := 0
	s.AwaitHoldsTrue(func() bool {
		attempts++
		return true
	}, time.Second, 400*time.Millisecond)
	s.Equal(3, attempts)
	s.Equal(time.Second, time.Since(start))
	s.AwaitHoldsTruef(func() bool { return true }, time.Second, time.Second, "counter %s", "unchanged")
	s.Equal(2*time.Second, time.Since(start))
}

func TestRunHolds(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := &holdsSuite{}
		s.initSuite(t, false, nil, nil)
		s.TestAwaitHolds()
		s.TestAwaitHoldsf()
		s.TestAwaitHoldsTrue()
	})
}
