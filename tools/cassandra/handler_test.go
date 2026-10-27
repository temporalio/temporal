package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"
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
	s.NotNil(validateCQLClientConfig(config))

	config.Hosts = environment.GetCassandraAddress()
	s.NotNil(validateCQLClientConfig(config))

	config.Keyspace = "foobar"
	s.Nil(validateCQLClientConfig(config))
}

func (s *HandlerTestSuite) TestParsingOfOptionsMap() {
	parsedMap := parseOptionsMap("key1=value1 ,key2= value2,key3=value3")

	s.Assert().Equal("value1", parsedMap["key1"])
	s.Assert().Equal("value2", parsedMap["key2"])
	s.Assert().Equal("value3", parsedMap["key3"])
	s.Assert().Equal("", parsedMap["key4"])

	parsedMap2 := parseOptionsMap("key1=,=value2")

	s.Assert().Equal(0, len(parsedMap2))
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
	s.Nil(err)
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
	s.Nil(err)
}

func (s *HandlerTestSuite) TestCreateKeyspaceDatacenter() {
	testCases := []struct {
		name     string
		env      string
		args     []string
		expected string
	}{
		{
			name:     "not set",
			args:     []string{"create-keyspace", "-k", "foo"},
			expected: "",
		},
		{
			name:     "command flag",
			args:     []string{"create-keyspace", "-k", "foo", "--dc", "dc1"},
			expected: "dc1",
		},
		{
			name:     "global flag",
			args:     []string{"--dc", "dc1", "create-keyspace", "-k", "foo"},
			expected: "dc1",
		},
		{
			name:     "env var",
			env:      "dc1",
			args:     []string{"create-keyspace", "-k", "foo"},
			expected: "dc1",
		},
		{
			name:     "command flag overrides global flag",
			args:     []string{"--dc", "dc1", "create-keyspace", "-k", "foo", "--dc", "dc2"},
			expected: "dc2",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.T().Setenv("CASSANDRA_DATACENTER", tc.env)

			var datacenter string
			app := buildCLIOptions()
			for i := range app.Commands {
				if app.Commands[i].Name == "create-keyspace" {
					app.Commands[i].Action = func(c *cli.Context) {
						datacenter = keyspaceDatacenter(c)
					}
				}
			}

			s.NoError(app.Run(append([]string{"./tool"}, tc.args...)))
			s.Equal(tc.expected, datacenter)
		})
	}
}
