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

package tdbg

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func (s *utilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}
func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

type utilSuite struct {
	*require.Assertions
	suite.Suite
}

func (s *utilSuite) TestStringToEnum_MapCaseInsensitive() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := stringToEnum("timeR", enumValues)
	s.NoError(err)
	s.Equal(result, int32(2)) // Timer
}

func (s *utilSuite) TestStringToEnum_MapNonExisting() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := stringToEnum("Timer2", enumValues)
	s.Error(err)
	s.Equal(result, int32(0))
}

func (s *utilSuite) TestStringToEnum_MapEmptyValue() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := stringToEnum("", enumValues)
	s.NoError(err)
	s.Equal(result, int32(0))
}

func (s *utilSuite) TestStringToEnum_MapEmptyEnum() {
	enumValues := map[string]int32{}

	result, err := stringToEnum("Timer", enumValues)
	s.Error(err)
	s.Equal(result, int32(0))
}
