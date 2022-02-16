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

package log

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/log"

	"go.temporal.io/server/common/log/tag"
)

type SdkLoggerSuite struct {
	*require.Assertions
	suite.Suite

	controller       *gomock.Controller
	sdkLogger        log.Logger
	underlyingLogger *MockLogger
}

func TestSdkLoggerSuite(t *testing.T) {
	suite.Run(t, &SdkLoggerSuite{})
}

func (s *SdkLoggerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.underlyingLogger = NewMockLogger(s.controller)
	s.sdkLogger = NewSdkLogger(s.underlyingLogger)
}

func (s *SdkLoggerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *SdkLoggerSuite) TestEvenKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2", "val2")
}

func (s *SdkLoggerSuite) TestOddKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewAnyTag("key2", "no value"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2")
}

func (s *SdkLoggerSuite) TestKeyValPairsWithTag() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewStringTag("key3", "val3"), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.NewStringTag("key3", "val3"), "key2", "val2")

	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewInt("key3", 3), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.NewInt("key3", 3), "key2", "val2")

}

func (s *SdkLoggerSuite) TestEmptyKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg")
	s.sdkLogger.Info("msg")
}

func (s *SdkLoggerSuite) TestSingleKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "no value"))
	s.sdkLogger.Info("msg", "key1")
}
