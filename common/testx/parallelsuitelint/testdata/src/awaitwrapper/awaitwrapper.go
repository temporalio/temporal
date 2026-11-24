package awaitwrapper

import (
	"time"

	forwardawait "example.com/await"
	"example.com/otherawait"
	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }

func awaiting(s *testSuite, ch chan int) {
	forwardawait.Requiref(s.Context(), s.TB(), func(*forwardawait.T) {}, time.Second, time.Millisecond, "waiting") // want "use suite Awaitf instead of await.Requiref"
	forwardawait.Rcv[int](s.T(), ch)                                                                               // want "use suite Rcv instead of await.Rcv"
	forwardawait.Snd(s.TB(), ch, 1)                                                                                // want "use suite Snd instead of await.Snd"

	ready := func() bool { return true }
	forwardawait.RequireTruef(s.T(), ready, time.Second, time.Millisecond, "waiting")
	otherawait.RequireTrue(s.T(), ready, time.Second, time.Millisecond)
}
