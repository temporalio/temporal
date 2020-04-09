package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/tools/common/schema"
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
	s.NotNil(validateCQLClientConfig(config, false))
	s.NotNil(validateCQLClientConfig(config, true))

	config.Hosts = environment.GetCassandraAddress()
	s.NotNil(validateCQLClientConfig(config, false))
	s.Nil(validateCQLClientConfig(config, true))
	s.Equal(schema.DryrunDBName, config.Keyspace)

	config.Keyspace = "foobar"
	s.Nil(validateCQLClientConfig(config, false))
	s.Nil(validateCQLClientConfig(config, true))
}
