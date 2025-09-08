package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	HandlerTestSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HandlerTestSuite) TestFlag() {
	result := flag("test-option")
	s.Equal("(--test-option)", result)
}
