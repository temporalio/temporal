// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package clitest

import (
	"net"
	"strconv"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/auth"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/tools/common/schema"
	"github.com/uber/cadence/tools/sql"
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
