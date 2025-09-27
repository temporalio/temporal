package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	MainTestSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestMainTestSuite(t *testing.T) {
	suite.Run(t, new(MainTestSuite))
}

func (s *MainTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

// Test CLI error scenarios similar to cassandra tests
func (s *MainTestSuite) TestSetupSchemaError() {
	// fake exit function to avoid exiting the application
	back := osExit
	defer func() { osExit = back }()
	osExit = func(i int) {
		s.Equal(1, i)
	}
	args := []string{"./tool", "setup-schema"}
	app := BuildCLIOptions()
	err := app.Run(args)
	s.Nil(err)
}

func (s *MainTestSuite) TestPingError() {
	// fake exit function to avoid exiting the application
	back := osExit
	defer func() { osExit = back }()
	osExit = func(i int) {
		s.Equal(1, i)
	}
	args := []string{"./tool", "--endpoint", "http://nonexistent:9200", "ping"}
	app := BuildCLIOptions()
	err := app.Run(args)
	s.Nil(err)
}
