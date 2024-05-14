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

package faultinjection

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	methodFaultGeneratorSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestMethodFaultGeneratorSuite(t *testing.T) {
	s := new(methodFaultGeneratorSuite)
	suite.Run(t, s)
}

func (s *methodFaultGeneratorSuite) SetupSuite() {}

func (s *methodFaultGeneratorSuite) TearDownSuite() {}

func (s *methodFaultGeneratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *methodFaultGeneratorSuite) TearDownTest() {}

func (s *methodFaultGeneratorSuite) Test_Generate() {
	faults := []fault{
		{
			err:    errors.New("random error0"),
			execOp: false,
			rate:   0.01,
		},
		{
			err:    errors.New("random error1"),
			execOp: false,
			rate:   0.11,
		},
		{
			err:    errors.New("random error2"),
			execOp: false,
			rate:   0.22,
		},
	}
	gen := newMethodFaultGenerator(faults, 2208)

	s.EqualValues(34, math.Round(gen.rate*100))
	s.Len(gen.faultsMetadata, 3)
	s.EqualValues(1, math.Round(gen.faultsMetadata[0].threshold*100))
	s.EqualValues(12, math.Round(gen.faultsMetadata[1].threshold*100))
	s.EqualValues(34, math.Round(gen.faultsMetadata[2].threshold*100))

	f1 := gen.generate()
	s.Nil(f1)
	f2 := gen.generate()
	s.NotNil(f2)
	s.Equal(faults[2], *f2)
	f3 := gen.generate()
	s.NotNil(f3)
	s.Equal(faults[2], *f3)
	f4 := gen.generate()
	s.Nil(f4)
}
