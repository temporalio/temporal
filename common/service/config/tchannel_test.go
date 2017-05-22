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

package config

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/tchannel-go"
	"testing"
)

type TChannelSuite struct {
	*require.Assertions
	suite.Suite
}

func TestTChannelSuite(t *testing.T) {
	suite.Run(t, new(TChannelSuite))
}

func (s *TChannelSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *TChannelSuite) TestParseLogLevel() {
	var cfg TChannel
	cfg.LogLevel = "debug"
	s.Equal(tchannel.LogLevelDebug, cfg.getLogLevel())
	cfg.LogLevel = "info"
	s.Equal(tchannel.LogLevelInfo, cfg.getLogLevel())
	cfg.LogLevel = "warn"
	s.Equal(tchannel.LogLevelWarn, cfg.getLogLevel())
	cfg.LogLevel = "error"
	s.Equal(tchannel.LogLevelError, cfg.getLogLevel())
	cfg.LogLevel = "fatal"
	s.Equal(tchannel.LogLevelFatal, cfg.getLogLevel())
	cfg.LogLevel = ""
	s.Equal(tchannel.LogLevelWarn, cfg.getLogLevel())
}

func (s *TChannelSuite) TestFactory() {
	cfg := &TChannel{
		LogLevel:        "info",
		BindOnLocalHost: true,
	}
	f := cfg.NewFactory()
	s.NotNil(f)
	ch, _ := f.CreateChannel("test", nil)
	s.NotNil(ch)
	ch.Close()
}
