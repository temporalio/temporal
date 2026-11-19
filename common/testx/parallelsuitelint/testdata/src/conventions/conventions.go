package conventions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	serverawait "go.temporal.io/server/common/testing/await"
	poll "go.temporal.io/testx/await"
	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }
type alias = testSuite
type wrapperSuite struct{ *testSuite }

func (other *testSuite) wrongReceiver()      {} // want "suite receiver must be named s"
func (*testSuite) unnamedReceiver()          {} // want "suite receiver must be named s"
func (other *wrapperSuite) wrapperReceiver() {} // want "suite receiver must be named s"
func (s *alias) correctReceiver()            { s.Equal(1, 1) }

func callbacks(s *testSuite) {
	s.Run("child", func(child *testSuite) { child.Equal(1, 1) })           // want "suite callback parameter must be named s"
	s.RunSequential("child", func(child *testSuite) { child.Equal(1, 1) }) // want "suite callback parameter must be named s"
	s.Run("nested", func(s *testSuite) {
		s.Run("child", func(s *testSuite) { s.Equal(1, 1) })
	})
}

func attempts(s *testSuite) {
	s.Await(func(child *testSuite) { child.Equal(1, 1) }, time.Second, time.Millisecond)             // want "suite callback parameter must be named s"
	s.Awaitf(func(child *testSuite) { child.Equal(1, 1) }, time.Second, time.Millisecond, "waiting") // want "suite callback parameter must be named s"
	s.Await(func(s *testSuite) { s.Equal(1, 1) }, time.Second, time.Millisecond)
	s.Equal(1, 1)
}

func awaiting(s, other *testSuite, t *testing.T, ctx context.Context, ch chan int) {
	attempt := func(*poll.T) {}
	ready := func() bool { return true }

	poll.Require(s.Context(), s.T(), attempt, time.Second, time.Millisecond)                       // want "use suite Await instead of await.Require"
	serverawait.Requiref((s).Context(), s.TB(), attempt, time.Second, time.Millisecond, "waiting") // want "use suite Awaitf instead of await.Requiref"
	poll.RequireTrue(s.TB(), ready, time.Second, time.Millisecond)                                 // want "use suite AwaitTrue instead of await.RequireTrue"
	poll.RequireTruef(s.T(), ready, time.Second, time.Millisecond, "waiting")                      // want "use suite AwaitTruef instead of await.RequireTruef"
	poll.Rcv[int](s.T(), ch)                                                                       // want "use suite Rcv instead of await.Rcv"
	poll.Snd(s.TB(), ch, 1)                                                                        // want "use suite Snd instead of await.Snd"

	poll.Require(ctx, s.T(), attempt, time.Second, time.Millisecond)
	poll.Require(other.Context(), s.T(), attempt, time.Second, time.Millisecond)
	poll.Require(s.Context(), t, attempt, time.Second, time.Millisecond)
	poll.RequireTrue(t, ready, time.Second, time.Millisecond)
	poll.Rcv(t, ch)
	s.Await(func(s *testSuite) {}, time.Second, time.Millisecond)
	s.AwaitTrue(ready, time.Second, time.Millisecond)
	s.Rcv(ch)
}

func assertions(s *testSuite) {
	require.Equal(s.T(), 1, 1)          // want "use suite assertion methods directly"
	assert.Equal(s.T(), 1, 1)           // want "use suite assertion methods directly"
	require.NoError(s.TB(), nil)        // want "use suite assertion methods directly"
	require.Equal(s.AssertionT(), 1, 1) // want "use suite assertion methods directly"
	s.Require().Equal(1, 1)             // want "use suite assertion methods directly"
	s.Assertions.Equal(1, 1)            // want "use suite assertion methods directly"
	s.Equal(1, 1)
	s.NoError(nil)
	s.Equal(s.T(), s.T())
	useAssertions(s.Require())
	s.T().Log("allowed")
	s.T().Helper()
}

func bypasses(s *testSuite) {
	s.T().Run("child", func(t *testing.T) {}) // want "use suite Run or RunSequential"
	s.T().Parallel()                          // want "do not call Parallel on a suite test handle"
}

func rawAssertions(s *testSuite) {
	s.T().Error("failure")           // want "use suite assertion methods directly"
	s.TB().Fatal("failure")          // want "use suite assertion methods directly"
	s.AssertionT().Errorf("failure") // want "use suite assertion methods directly"
	s.AssertionT().FailNow()         // want "use suite assertion methods directly"
}

func disabledMethods(s *testSuite) {
	s.Assert()    // want "Assert is unsupported by parallelsuite"
	s.SetT(s.T()) // want "SetT is unsupported by parallelsuite"
	s.SetS(s)     // want "SetS is unsupported by parallelsuite"
}

func scopeHelpers(s ps.Scope) {
	require.Equal(s.AssertionT(), 1, 1)
	s.Require().Equal(1, 1)
	s.TB().Log("allowed")
	s.AssertionT().Errorf("allowed for scope helpers")
}

type unrelated struct{}

func (other *unrelated) method()                {}
func (*unrelated) Run(string, func(*testSuite)) {}

func unrelatedCalls(other *unrelated, t *testing.T) {
	other.Run("child", func(child *testSuite) {})
	require.Equal(t, 1, 1)
	assert.Equal(t, 1, 1)
	t.Run("child", func(t *testing.T) {})
	t.Parallel()
}

func useAssertions(*require.Assertions) {}
