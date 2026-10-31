package await_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

type retrySuite struct {
	*require.Assertions
	await.Suite[*retrySuite]
	ctx context.Context
	t   *testing.T
}

func (s *retrySuite) Context() context.Context { return s.ctx }
func (s *retrySuite) TB() testing.TB           { return s.t }

func TestSuiteRetries(t *testing.T) {
	t.Parallel()
	var copySuite func(*await.T) *retrySuite
	copySuite = func(at *await.T) *retrySuite {
		s := &retrySuite{Assertions: require.New(at), ctx: at.Context(), t: t}
		s.Suite = await.NewSuite(s, copySuite)
		return s
	}
	s := &retrySuite{Assertions: require.New(t), ctx: t.Context(), t: t}
	s.Suite = await.NewSuite(s, copySuite)
	var attempts atomic.Int32
	s.Await(func(at *retrySuite) {
		at.NotSame(s, at)
		at.NotSame(s.Context(), at.Context())
		at.Equal(int32(2), attempts.Add(1))
	}, time.Second, time.Millisecond)
	s.Equal(int32(2), attempts.Load())
}
