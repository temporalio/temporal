package clitest

import (
	"net"
	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/auth"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/tools/common/schema"
	"github.com/temporalio/temporal/tools/sql"
)

type (
	// HandlerTestSuite defines a test suite
	HandlerTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		pluginName string
	}
)

// NewHandlerTestSuite returns a test suite
func NewHandlerTestSuite(pluginName string) *HandlerTestSuite {
	return &HandlerTestSuite{
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

	s.NotNil(sql.ValidateConnectConfig(cfg, false))
	s.NotNil(sql.ValidateConnectConfig(cfg, true))

	cfg.ConnectAddr = net.JoinHostPort(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
	)
	s.NotNil(sql.ValidateConnectConfig(cfg, false))
	s.Nil(sql.ValidateConnectConfig(cfg, true))
	s.Equal(schema.DryrunDBName, cfg.DatabaseName)

	cfg.DatabaseName = "foobar"
	s.Nil(sql.ValidateConnectConfig(cfg, false))
	s.Nil(sql.ValidateConnectConfig(cfg, true))

	cfg.TLS = &auth.TLS{}
	cfg.TLS.Enabled = true
	s.NotNil(sql.ValidateConnectConfig(cfg, false))
	s.NotNil(sql.ValidateConnectConfig(cfg, true))

	cfg.TLS.CaFile = "ca.pem"
	s.Nil(sql.ValidateConnectConfig(cfg, false))
	s.Nil(sql.ValidateConnectConfig(cfg, true))

	cfg.TLS.KeyFile = "key_file"
	cfg.TLS.CertFile = ""
	s.NotNil(sql.ValidateConnectConfig(cfg, false))
	s.NotNil(sql.ValidateConnectConfig(cfg, true))

	cfg.TLS.KeyFile = ""
	cfg.TLS.CertFile = "cert_file"
	s.NotNil(sql.ValidateConnectConfig(cfg, false))
	s.NotNil(sql.ValidateConnectConfig(cfg, true))

	cfg.TLS.KeyFile = "key_file"
	cfg.TLS.CertFile = "cert_file"
	s.Nil(sql.ValidateConnectConfig(cfg, false))
	s.Nil(sql.ValidateConnectConfig(cfg, true))
}
