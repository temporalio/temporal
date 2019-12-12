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

package cadence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type CadenceSuite struct {
	*require.Assertions
	suite.Suite
}

func TestCadenceSuite(t *testing.T) {
	suite.Run(t, new(CadenceSuite))
}

func (s *CadenceSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *CadenceSuite) TestIsValidService() {
	s.True(isValidService("history"))
	s.True(isValidService("matching"))
	s.True(isValidService("frontend"))
	s.False(isValidService("cadence-history"))
	s.False(isValidService("cadence-matching"))
	s.False(isValidService("cadence-frontend"))
	s.False(isValidService("foobar"))
}

func (s *CadenceSuite) TestPath() {
	s.Equal("foo/bar", constructPath("foo", "bar"))
}
