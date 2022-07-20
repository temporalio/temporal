// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
)

type (
	tlsConfigTest struct {
		suite.Suite
		*require.Assertions
	}
)

func TestTLSConfigSuite(t *testing.T) {
	s := new(tlsConfigTest)
	suite.Run(t, s)
}

func (s *tlsConfigTest) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *tlsConfigTest) TestIsEnabled() {

	emptyCfg := config.GroupTLS{}
	s.False(emptyCfg.IsServerEnabled())
	s.False(emptyCfg.IsClientEnabled())
	cfg := config.GroupTLS{Server: config.ServerTLS{KeyFile: "foo"}}
	s.True(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Server: config.ServerTLS{KeyData: "foo"}}
	s.True(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: true}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: false}}
	s.False(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())

}

func (s *tlsConfigTest) TestIsSystemWorker() {

	cfg := &config.RootTLS{}
	s.False(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertFile: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertData: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: true}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: false}}}
	s.False(isSystemWorker(cfg))
}

func (s *tlsConfigTest) TestCertFileAndData() {
	s.testGroupTLS(s.testCertFileAndData)
}

func (s *tlsConfigTest) TestKeyFileAndData() {
	s.testGroupTLS(s.testKeyFileAndData)
}

func (s *tlsConfigTest) TestClientCAData() {
	s.testGroupTLS(s.testClientCAData)
}

func (s *tlsConfigTest) TestClientCAFiles() {
	s.testGroupTLS(s.testClientCAFiles)
}

func (s *tlsConfigTest) TestRootCAData() {
	s.testGroupTLS(s.testRootCAData)
}

func (s *tlsConfigTest) TestRootCAFiles() {
	s.testGroupTLS(s.testRootCAFiles)
}

func (s *tlsConfigTest) testGroupTLS(f func(*config.RootTLS, *config.GroupTLS)) {

	cfg := &config.RootTLS{Internode: config.GroupTLS{}}
	f(cfg, &cfg.Internode)
	cfg = &config.RootTLS{Frontend: config.GroupTLS{}}
	f(cfg, &cfg.Frontend)
}

func (s *tlsConfigTest) testCertFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertData: "bar"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo", CertData: "bar"}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testKeyFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyData: "bar"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo", KeyData: "bar"}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) TestSystemWorkerTLSConfig() {
	cfg := &config.RootTLS{}
	cfg.SystemWorker = config.WorkerTLS{}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertData: "bar"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo", CertData: "bar"}
	s.Error(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyData: "bar"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo", KeyData: "bar"}
	s.Error(validateRootTLS(cfg))

	cfg.SystemWorker = config.WorkerTLS{Client: config.ClientTLS{}}
	client := &cfg.SystemWorker.Client
	client.RootCAData = []string{}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo"}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo", "bar"}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo", " "}
	s.Error(validateRootTLS(cfg))
	client.RootCAData = []string{""}
	s.Error(validateRootTLS(cfg))
}
