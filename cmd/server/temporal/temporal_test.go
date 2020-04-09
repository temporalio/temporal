package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TemporalSuite struct {
	*require.Assertions
	suite.Suite
}

func TestTemporalSuite(t *testing.T) {
	suite.Run(t, new(TemporalSuite))
}

func (s *TemporalSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *TemporalSuite) TestIsValidService() {
	s.True(isValidService("history"))
	s.True(isValidService("matching"))
	s.True(isValidService("frontend"))
	s.False(isValidService("temporal-history"))
	s.False(isValidService("temporal-matching"))
	s.False(isValidService("temporal-frontend"))
	s.False(isValidService("foobar"))
}

func (s *TemporalSuite) TestPath() {
	s.Equal("foo/bar", constructPath("foo", "bar"))
}
