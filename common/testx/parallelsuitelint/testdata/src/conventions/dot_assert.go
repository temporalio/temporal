package conventions

import . "github.com/stretchr/testify/assert"

func assertAssertions(s *testSuite) {
	Equal(s.T(), 1, 2) // want "use suite assertion methods directly"
}
