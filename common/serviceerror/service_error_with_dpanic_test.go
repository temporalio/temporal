// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package serviceerror

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
)

type serviceErrorWithDPanicSuite struct {
	*require.Assertions
	suite.Suite
}

func TestServiceErrorWithDPanicSuite(t *testing.T) {
	suite.Run(t, new(serviceErrorWithDPanicSuite))
}

func (s *serviceErrorWithDPanicSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *serviceErrorWithDPanicSuite) TestNewDPanicInProd() {
	cfg := log.Config{
		Level: "info",
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	s.NotNil(logger)

	err := NewInternalErrorWithDPanic(logger, "Must not panic!")
	s.NotNil(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
}

func (s *serviceErrorWithDPanicSuite) TestNewDPanicInDev() {
	cfg := log.Config{
		Level:       "info",
		Development: true,
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	s.NotNil(logger)

	s.Panics(nil, func() {
		err := NewInternalErrorWithDPanic(logger, "Must panic!")
		s.Nil(err)
	})
}
