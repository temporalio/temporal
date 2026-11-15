package conventions

import (
	. "github.com/stretchr/testify/require"
	ps "go.temporal.io/testx/parallelsuite"
)

func requireAssertions(s *testSuite) {
	Equal(s.T(), 1, 2) // want "use suite assertion methods directly"
	NoError(s.AssertionT(), nil) // want "use suite assertion methods directly"
}

func scopeAssertions(s ps.Scope) {
	Equal(s.AssertionT(), 1, 1)
}
