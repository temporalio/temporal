package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	HandlerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		db *mockSQLDB
	}
)

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HandlerTestSuite))
}

func (s *HandlerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.db = new(mockSQLDB)
}

func (s *HandlerTestSuite) TestValidateSetupConfig() {
	config := new(SetupConfig)
	s.assertValidateSetupFails(config, s.db)

	s.assertValidateSetupFails(config, s.db)

	config.InitialVersion = "0.1"
	config.DisableVersioning = true
	config.SchemaFilePath = ""
	s.assertValidateSetupFails(config, s.db)

	config.InitialVersion = "0.1"
	config.DisableVersioning = true
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupFails(config, s.db)

	config.InitialVersion = ""
	config.DisableVersioning = true
	config.SchemaFilePath = ""
	s.assertValidateSetupFails(config, s.db)

	config.InitialVersion = "0.1"
	config.DisableVersioning = false
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupSucceeds(config, s.db)

	config.InitialVersion = "0.1"
	config.DisableVersioning = false
	config.SchemaFilePath = ""
	s.assertValidateSetupSucceeds(config, s.db)

	config.InitialVersion = ""
	config.DisableVersioning = true
	config.SchemaFilePath = "/tmp/foo.cql"
	s.assertValidateSetupSucceeds(config, s.db)

	config.SchemaName = "mysql/v8/temporal"
	s.assertValidateSetupFails(config, s.db)
	config.SchemaFilePath = ""
	s.assertValidateSetupSucceeds(config, s.db)
	config.SchemaName = "foo"
	s.assertValidateSetupFails(config, s.db)
}

func (s *HandlerTestSuite) TestValidateUpdateConfig() {

	config := new(UpdateConfig)
	s.assertValidateUpdateFails(config, s.db)

	s.assertValidateUpdateFails(config, s.db)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "abc"
	s.assertValidateUpdateFails(config, s.db)

	config.SchemaDir = "/tmp"
	config.TargetVersion = ""
	s.assertValidateUpdateSucceeds(config, s.db)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "1.2"
	s.assertValidateUpdateSucceeds(config, s.db)

	config.SchemaDir = "/tmp"
	config.TargetVersion = "v1.2"
	s.assertValidateUpdateSucceeds(config, s.db)
	s.Equal("1.2", config.TargetVersion)

	config.SchemaName = "mysql/v8/temporal"
	s.assertValidateUpdateFails(config, s.db)
	config.SchemaDir = ""
	s.assertValidateUpdateSucceeds(config, s.db)
	config.SchemaName = "foo"
	s.assertValidateUpdateFails(config, s.db)
}

func (s *HandlerTestSuite) assertValidateSetupSucceeds(input *SetupConfig, db DB) {
	err := validateSetupConfig(input, db)
	s.Nil(err)
}

func (s *HandlerTestSuite) assertValidateSetupFails(input *SetupConfig, db DB) {
	err := validateSetupConfig(input, db)
	s.NotNil(err)
	_, ok := err.(*ConfigError)
	s.True(ok)
}

func (s *HandlerTestSuite) assertValidateUpdateSucceeds(input *UpdateConfig, db DB) {
	err := validateUpdateConfig(input, db)
	s.Nil(err)
}

func (s *HandlerTestSuite) assertValidateUpdateFails(input *UpdateConfig, db DB) {
	err := validateUpdateConfig(input, db)
	s.NotNil(err)
	_, ok := err.(*ConfigError)
	s.True(ok)
}
