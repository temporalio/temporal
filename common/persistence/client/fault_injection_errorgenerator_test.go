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

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	errorGeneratorSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestErrorGeneratorSuite(t *testing.T) {
	s := new(errorGeneratorSuite)
	suite.Run(t, s)
}

func (s *errorGeneratorSuite) SetupSuite() {}

func (s *errorGeneratorSuite) TearDownSuite() {}

func (s *errorGeneratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

}

func (s *errorGeneratorSuite) TearDownTest() {}

func (s *errorGeneratorSuite) TestZeroRateCanUpdateToNonZero() {
	testObject := NewDefaultErrorGenerator(0.0, defaultErrors)
	testObject.UpdateRate(1)
	s.NotNil(testObject.Generate())
}

func (s *errorGeneratorSuite) TestAfterUpdateToZeroRateGenerateReturnsNoErrors() {
	testObject := NewDefaultErrorGenerator(1.0, defaultErrors)
	testObject.UpdateRate(0)
	s.Nil(testObject.Generate())
}
