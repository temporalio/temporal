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

package blob

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type KeySuite struct {
	*require.Assertions
	suite.Suite
}

func TestKeySuite(t *testing.T) {
	suite.Run(t, new(KeySuite))
}

func (s *KeySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *KeySuite) TestNewKey() {
	testCases := []struct {
		extension      string
		pieces         []string
		expectError    bool
		expectBuiltKey string
	}{
		{
			extension:   "ext",
			pieces:      []string{},
			expectError: true,
		},
		{
			extension:   "ext",
			pieces:      []string{"1", "2", "3", "4", "5"},
			expectError: true,
		},
		{
			extension:   "invalid_extension.",
			pieces:      []string{"foo", "bar"},
			expectError: true,
		},
		{
			extension:   "ext",
			pieces:      []string{"invalid=piece"},
			expectError: true,
		},
		{
			extension:   longString(60),
			pieces:      []string{longString(60), longString(60), longString(60), longString(60)},
			expectError: true,
		},
		{
			extension:   "",
			pieces:      []string{"1"},
			expectError: true,
		},
		{
			extension:   "ext",
			pieces:      []string{"1", ""},
			expectError: true,
		},
		{
			extension:      "ext",
			pieces:         []string{"valid", "set", "of", "pieces"},
			expectError:    false,
			expectBuiltKey: "valid_set_of_pieces.ext",
		},
	}

	for _, tc := range testCases {
		key, err := NewKey(tc.extension, tc.pieces...)
		if tc.expectError {
			s.Error(err)
			s.Nil(key)
		} else {
			s.NoError(err)
			s.NotNil(key)
			s.Equal(tc.expectBuiltKey, key.String())
		}
	}
}

func (s *KeySuite) TestNewKeyFromString() {
	testCases := []struct {
		inputStr         string
		expectError      bool
		expectBuiltKey   string
		expectExtension  string
		expectNamePieces []string
	}{
		{
			inputStr:    "",
			expectError: true,
		},
		{
			inputStr:    "name.",
			expectError: true,
		},
		{
			inputStr:    ".ext",
			expectError: true,
		},
		{
			inputStr:    "ext",
			expectError: true,
		},
		{
			inputStr:    "foo.bar.baz.ext",
			expectError: true,
		},
		{
			inputStr:    "1_2_3_4_5.ext",
			expectError: true,
		},
		{
			inputStr:    "1=4_5.ext",
			expectError: true,
		},
		{
			inputStr:    "foo_bar_bax.e,x,t,",
			expectError: true,
		},
		{
			inputStr:         "foo1_bar2_3baz.ext",
			expectError:      false,
			expectBuiltKey:   "foo1_bar2_3baz.ext",
			expectExtension:  "ext",
			expectNamePieces: []string{"foo1", "bar2", "3baz"},
		},
	}

	for _, tc := range testCases {
		key, err := NewKeyFromString(tc.inputStr)
		if tc.expectError {
			s.Error(err)
			s.Nil(key)
		} else {
			s.NoError(err)
			s.NotNil(key)
			s.Equal(tc.expectBuiltKey, key.String())
			s.Equal(tc.expectNamePieces, key.Pieces())
			s.Equal(tc.expectExtension, key.Extension())
		}
	}
}

func longString(length int) string {
	var result string
	for i := 0; i < length; i++ {
		result += "a"
	}
	return result
}
