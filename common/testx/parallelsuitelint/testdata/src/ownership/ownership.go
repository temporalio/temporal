package ownership

import (
	"time"

	ps "go.temporal.io/testx/parallelsuite"
)

type suite struct{ ps.Suite[*suite] }
type alias = suite

func captures(s *alias) {
	parent := s
	s.Equal(1, 1)
	s.Run("child", func(child *suite) { // want "suite callback parameter must be named s"
		child.Equal(1, 1)
		s.Equal(1, 1)                                 // want "receiver is sealed"
		parent.T()                                    // want "receiver is sealed"
		child.Run("nested", func(grandchild *suite) { // want "suite callback parameter must be named s"
			grandchild.Equal(1, 1)
			child.Equal(1, 1)  // want "receiver is sealed"
			parent.Equal(1, 1) // want "receiver is sealed"
		})
		child.T() // want "receiver is sealed"
	})
	s.Run("sibling", func(s *suite) { s.Equal(1, 1) })
	s.Equal(1, 1) // want "receiver is sealed"
}

func branches(s *suite, b bool) {
	if b {
		s.Run("child", func(s *suite) { s.T() })
	}
	s.T() // want "receiver is sealed"
}

func earlyReturn(s *suite, b bool) {
	if b {
		s.Run("child", func(s *suite) { s.T() })
		return
	}
	s.T()
}

func instances(s, other *suite) {
	s.Run("child", func(child *suite) { other.T() }) // want "suite callback parameter must be named s"
	other.Equal(1, 1)
	{
		s := other
		s.T()
	}
}

type unrelated struct{}

func (*unrelated) Run(string, func(*unrelated)) {}
func (*unrelated) T()                           {}
func unrelatedRun(s *unrelated) {
	s.Run("child", func(child *unrelated) { s.T() })
	s.T()
}

// Dynamic aliases and helper calls remain the runtime guard's responsibility.
func limits(s, other *suite) {
	p := s
	p = other
	s.Run("child", func(child *suite) { p.T(); helper(s) }) // want "suite callback parameter must be named s"
	callback := func(child *suite) { s.T() }
	s.Run("dynamic", callback)
	method := s.T
	method()
}
func helper(s *suite) { s.T() }

func parenthesizedAlias(s *suite) {
	parent := (s)
	s.Run("child", func(s *suite) {})
	parent.Equal(1, 1) // want "receiver is sealed"
}

func deferred(s *suite) {
	defer s.Run("later", func(child *suite) { child.T() }) // want "suite callback parameter must be named s"
	s.T()
}

func asynchronous(s *suite) {
	go s.Run("concurrent", func(child *suite) { child.T() }) // want "suite callback parameter must be named s"
	s.T()
}

func loop(s *suite, b bool) {
	for b {
		s.T() // want "receiver is sealed"
		s.Run("child", func(s *suite) { s.T() })
	}
}

func freshInLoop(b bool) {
	for b {
		s := new(suite)
		s.T()
		s.Run("child", func(s *suite) { s.T() })
	}
}

func sequential(s *suite) {
	s.RunSequential("child", func(s *suite) { s.Equal(1, 1) })
	s.Context()                                                              // want "receiver is sealed"
	s.TB()                                                                   // want "receiver is sealed"
	s.Await(func(s *suite) { s.Equal(1, 1) }, time.Second, time.Millisecond) // want "receiver is sealed"
	s.Run("sibling", func(s *suite) { s.Equal(1, 1) })
}

func guardedHelpers(s *suite, values chan int) {
	s.Run("child", func(s *suite) { s.Equal(1, 1) })
	s.Awaitf(func(s *suite) { s.Equal(1, 1) }, time.Second, time.Millisecond, "waiting") // want "receiver is sealed"
	s.AwaitTrue(func() bool { return true }, time.Second, time.Millisecond)              // want "receiver is sealed"
	s.AwaitTruef(func() bool { return true }, time.Second, time.Millisecond, "waiting")  // want "receiver is sealed"
	s.Rcv(values)                                                                        // want "receiver is sealed"
	s.Snd(values, 1)                                                                     // want "receiver is sealed"
	s.Rcv[int](values)                                                                   // want "receiver is sealed"
	s.Snd[int](values, 1)                                                                // want "receiver is sealed"
	_ = s.Require()                                                                      // want "receiver is sealed"
	_ = s.AssertionT()                                                                   // want "receiver is sealed"
}

func parenthesizedCallback(s *suite) {
	parent := s
	s.Run("child", (func(s *suite) {
		parent.T() // want "receiver is sealed"
	}))
}

func awaitOwnership(s *suite) {
	parent := s
	s.Await(func(s *suite) {
		s.Equal(1, 1)
		parent.Equal(1, 1) // want "use the callback suite inside Await"
		parent.Context()   // want "use the callback suite inside Await"
	}, time.Second, time.Millisecond)
	s.Equal(1, 1)

	s.Awaitf(func(s *suite) {
		parent.T() // want "use the callback suite inside Await"
		s.Equal(1, 1)
	}, time.Second, time.Millisecond, "waiting")
}
