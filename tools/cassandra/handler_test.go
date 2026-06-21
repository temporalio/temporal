package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/temporal/environment"
)

type (
	HandlerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *HandlerTestSuite) TestValidateCQLClientConfig() {
	config := new(CQLClientConfig)
	s.Error(validateCQLClientConfig(config))

	config.Hosts = environment.GetCassandraAddress()
	s.Error(validateCQLClientConfig(config))

	config.Keyspace = "foobar"
	s.NoError(validateCQLClientConfig(config))
}

func (s *HandlerTestSuite) TestParsingOfOptionsMap() {
	parsedMap := parseOptionsMap("key1=value1 ,key2= value2,key3=value3")

	s.Equal("value1", parsedMap["key1"])
	s.Equal("value2", parsedMap["key2"])
	s.Equal("value3", parsedMap["key3"])
	s.Assert().Empty(parsedMap["key4"])

	parsedMap2 := parseOptionsMap("key1=,=value2")

	s.Assert().Empty(parsedMap2)
}

func (s *HandlerTestSuite) TestDropKeyspaceError() {
	// fake exit function to avoid exiting the application
	back := osExit
	defer func() { osExit = back }()
	osExit = func(i int) {
		s.Equal(1, i)
	}
	args := []string{"./tool", "drop-keyspace", "-f", "--keyspace", ""}
	app := buildCLIOptions()
	err := app.Run(args)
	s.NoError(err)
}

func (s *HandlerTestSuite) TestCreateKeyspaceError() {
	// fake exit function to avoid exiting the application
	back := osExit
	defer func() { osExit = back }()
	osExit = func(i int) {
		s.Equal(1, i)
	}
	args := []string{"./tool", "create-keyspace", "--keyspace", ""}
	app := buildCLIOptions()
	err := app.Run(args)
	s.NoError(err)
}
