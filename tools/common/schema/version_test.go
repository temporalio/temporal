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

func (s *VersionTestSuite) TestNormalizeVersionStringSuccess() {

	validInputs := []string{"0", "1000", "9999", "0.1", "0.9", "99.9", "100.8"}
	for _, in := range validInputs {
		ans, err := normalizeVersionString(in)
		s.NoError(err, in)
		s.Equal(in, ans, in)

		withPrefix := "v" + in
		ans, err = normalizeVersionString(withPrefix)
		s.NoError(err, in)
		s.Equal(in, ans, in)
	}

}

func (s *VersionTestSuite) TestNormalizeVersionStringInvalidInput() {

	errInputs := []string{"1.2a", "ab", "5.11a"}
	for _, in := range errInputs {
		_, err := normalizeVersionString(in)
		s.Error(err, in)
		_, err = normalizeVersionString("v" + in)
		s.Errorf(err, in)
	}
}
