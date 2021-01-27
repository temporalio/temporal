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

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	VersionTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestVersionTestSuite(t *testing.T) {
	suite.Run(t, new(VersionTestSuite))
}

func (s *VersionTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *VersionTestSuite) TestCmpVersion() {

	s.Equal(0, cmpVersion("0", "0"))
	s.Equal(0, cmpVersion("999", "999"))
	s.Equal(0, cmpVersion("0.0", "0.0"))
	s.Equal(0, cmpVersion("0.999", "0.999"))
	s.Equal(0, cmpVersion("99.888", "99.888"))

	s.True(cmpVersion("0.1", "0") > 0)
	s.True(cmpVersion("0.5", "0.1") > 0)
	s.True(cmpVersion("1.1", "0.1") > 0)
	s.True(cmpVersion("1.1", "0.9") > 0)
	s.True(cmpVersion("1.1", "1.0") > 0)

	s.True(cmpVersion("0", "0.1") < 0)
	s.True(cmpVersion("0.1", "0.5") < 0)
	s.True(cmpVersion("0.1", "1.1") < 0)
	s.True(cmpVersion("0.9", "1.1") < 0)
	s.True(cmpVersion("1.0", "1.1") < 0)

	s.True(cmpVersion("0.1a", "0.5") < 0)
	s.True(cmpVersion("0.1", "0.5a") > 0)
	s.True(cmpVersion("ab", "cd") == 0)
}

func (s *VersionTestSuite) TestParseValidateVersion() {

	inputs := []string{"0", "1000", "9999", "0.1", "0.9", "99.9", "100.8"}
	for _, in := range inputs {
		s.execParseValidateTest(in, in, false)
		s.execParseValidateTest("v"+in, in, false)
	}

	errInputs := []string{"1.2a", "ab", "5.11a"}
	for _, in := range errInputs {
		s.execParseValidateTest(in, "", true)
		s.execParseValidateTest("v"+in, "", true)
	}
}

func (s *VersionTestSuite) execParseValidateTest(input string, output string, isErr bool) {
	ver, err := parseValidateVersion(input)
	if isErr {
		s.NotNil(err)
		return
	}
	s.Nil(err)
	s.Equal(output, ver)
}
