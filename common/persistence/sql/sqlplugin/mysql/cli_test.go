package mysql

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/tools/sql/clitest"
)

func TestSQLConnTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSQLConnTestSuite(PluginName))
}

func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewHandlerTestSuite(PluginName))
}

func TestSetupSchemaTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSetupSchemaTestSuite(PluginName))
}

func TestUpdateSchemaTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewUpdateSchemaTestSuite(PluginName))
}

func TestVersionTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewVersionTestSuite(PluginName))
}
