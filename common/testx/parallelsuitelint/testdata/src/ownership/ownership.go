package ownership

import ps "go.temporal.io/testx/parallelsuite"

type suite struct{ ps.Suite[*suite] }
type alias = suite

func captures(s *alias) {
	parent := s
	s.Equal(1, 1)
	s.Run("child", func(child *suite) { // want "suite callback parameter must be named s"
		child.Equal(1, 1)
		s.Equal(1, 1) // want "receiver is sealed"
		parent.T()    // want "receiver is sealed"
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
