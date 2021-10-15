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
	s.False(emptyCfg.IsEnabled())
	cfg := config.GroupTLS{Server: config.ServerTLS{KeyFile: "foo"}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Server: config.ServerTLS{KeyData: "foo"}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: true}}
	s.True(cfg.IsEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: false}}
	s.False(cfg.IsEnabled())
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
