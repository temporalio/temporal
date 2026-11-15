package shared

import (
	"time"

	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }

func sequential(s *testSuite) {
	s.RunSequential("child", func(s *testSuite) { s.Equal(1, 1) })
	s.Context() // want "receiver is sealed"
	s.TB() // want "receiver is sealed"
	s.Await(func(s *testSuite) { s.Equal(1, 1) }, time.Second, time.Millisecond) // want "receiver is sealed"
	s.Run("sibling", func(s *testSuite) { s.Equal(1, 1) })
}

func guardedHelpers(s *testSuite, values chan int) {
	s.Run("child", func(s *testSuite) { s.Equal(1, 1) })
	s.Awaitf(func(s *testSuite) { s.Equal(1, 1) }, time.Second, time.Millisecond, "waiting") // want "receiver is sealed"
	s.AwaitTrue(func() bool { return true }, time.Second, time.Millisecond) // want "receiver is sealed"
	s.AwaitTruef(func() bool { return true }, time.Second, time.Millisecond, "waiting") // want "receiver is sealed"
	s.Rcv(values) // want "receiver is sealed"
	s.Snd(values, 1) // want "receiver is sealed"
	s.Rcv[int](values) // want "receiver is sealed"
	s.Snd[int](values, 1) // want "receiver is sealed"
	_ = s.Require() // want "receiver is sealed"
	_ = s.AssertionT() // want "receiver is sealed"
}

func parenthesizedCallback(s *testSuite) {
	parent := s
	s.Run("child", (func(s *testSuite) {
		parent.T() // want "receiver is sealed"
	}))
}

func awaitOwnership(s *testSuite) {
	parent := s
	s.Await(func(s *testSuite) {
		s.Equal(1, 1)
		parent.Equal(1, 1) // want "use the callback suite inside Await"
		parent.Context() // want "use the callback suite inside Await"
	}, time.Second, time.Millisecond)
	s.Equal(1, 1)

	s.Awaitf(func(s *testSuite) {
		parent.T() // want "use the callback suite inside Await"
		s.Equal(1, 1)
	}, time.Second, time.Millisecond, "waiting")
}
