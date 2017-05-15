package cassandra

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
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

func (s *HandlerTestSuite) TestValidateSetupSchemaConfig() {

	config := new(SetupSchemaConfig)
	s.assertValidateSetupFails(config)

	config.CassHosts = "127.0.0.1"
	s.assertValidateSetupFails(config)

	config.CassKeyspace = "test-keyspace"
	s.assertValidateSetupFails(config)

	config.InitialVersion = "0.1"
	config.DisableVersioning = true
	config.SchemaFilePath = ""
	s.assertValidateSetupFails(config)

	config.InitialVersion = "0.1"
	config.DisableVersioning = true
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupFails(config)

	config.InitialVersion = ""
	config.DisableVersioning = true
	config.SchemaFilePath = ""
	s.assertValidateSetupFails(config)

	config.InitialVersion = "0.1"
	config.DisableVersioning = false
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupSucceeds(config)

	config.InitialVersion = "0.1"
	config.DisableVersioning = false
	config.SchemaFilePath = ""
	s.assertValidateSetupSucceeds(config)

	config.InitialVersion = ""
	config.DisableVersioning = true
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupSucceeds(config)
}

func (s *HandlerTestSuite) TestValidateUpdateSchemaConfig() {

	config := new(UpdateSchemaConfig)
	s.assertValidateUpdateFails(config)

	config.CassHosts = "127.0.0.1"
	s.assertValidateUpdateFails(config)

	config.CassKeyspace = "test-keyspace"
	s.assertValidateUpdateFails(config)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "abc"
	s.assertValidateUpdateFails(config)

	config.SchemaDir = "/tmp"
	config.TargetVersion = ""
	s.assertValidateUpdateSucceeds(config)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "1.2"
	s.assertValidateUpdateSucceeds(config)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "v1.2"
	s.assertValidateUpdateSucceeds(config)
	s.Equal("1.2", config.TargetVersion)
}

func (s *HandlerTestSuite) assertValidateSetupSucceeds(input *SetupSchemaConfig) {
	err := validateSetupSchemaConfig(input)
	s.Nil(err)
}

func (s *HandlerTestSuite) assertValidateSetupFails(input *SetupSchemaConfig) {
	err := validateSetupSchemaConfig(input)
	s.NotNil(err)
	_, ok := err.(*ConfigError)
	s.True(ok)
}

func (s *HandlerTestSuite) assertValidateUpdateSucceeds(input *UpdateSchemaConfig) {
	err := validateUpdateSchemaConfig(input)
	s.Nil(err)
}

func (s *HandlerTestSuite) assertValidateUpdateFails(input *UpdateSchemaConfig) {
	err := validateUpdateSchemaConfig(input)
	s.NotNil(err)
	_, ok := err.(*ConfigError)
	s.True(ok)
}
