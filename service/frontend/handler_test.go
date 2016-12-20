package frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type HandlerTestSuite struct {
	suite.Suite
	Handler WorkflowHandler
}

func (s *HandlerTestSuite) SetupTest() {
	s.Handler = WorkflowHandler{}
}

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) TestHealthEndpoint() {
	healthy, err := s.Handler.IsHealthy(nil)
	assert.NoError(s.T(), err, "Health check shouldn't return error")
	assert.True(s.T(), healthy, "Health check needs to work")
}
