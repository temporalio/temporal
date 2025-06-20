package clitest

import (
	"net"
	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/sql"
)

type (
	// HandlerTestSuite defines a test suite
	HandlerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		host       string
		port       string
		pluginName string
	}
)

// NewHandlerTestSuite returns a test suite
func NewHandlerTestSuite(
	host string,
	port string,
	pluginName string,
) *HandlerTestSuite {
	return &HandlerTestSuite{
		host:       host,
		port:       port,
		pluginName: pluginName,
	}
}

// SetupTest setups test
func (s *HandlerTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// TestValidateConnectConfig test
func (s *HandlerTestSuite) TestValidateConnectConfig() {
	cfg := new(config.SQL)

	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.ConnectAddr = net.JoinHostPort(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
	)
	s.NotNil(sql.ValidateConnectConfig(cfg))

	cfg.DatabaseName = "foobar"
	s.Nil(sql.ValidateConnectConfig(cfg))
}
