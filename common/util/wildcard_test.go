// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/util"
)

func TestWildCardStringToRegexp(t *testing.T) {
	re, err := util.WildCardStringToRegexp("a*z")
	require.NoError(t, err)
	require.Regexp(t, re, "az")
	require.Regexp(t, re, "abz")
	require.NotRegexp(t, re, "ab")

	_, err = util.WildCardStringToRegexp("")
	require.ErrorContains(t, err, "pattern cannot be empty")
}

func TestWildCardStringsToRegexp(t *testing.T) {
	re, err := util.WildCardStringsToRegexp([]string{"a*z", "b*d"})
	require.NoError(t, err)
	require.Regexp(t, re, "az")
	require.Regexp(t, re, "abz")
	require.NotRegexp(t, re, "ab")
	require.Regexp(t, re, "bd")
	require.Regexp(t, re, "bcd")
	require.NotRegexp(t, re, "bc")

	re, err = util.WildCardStringsToRegexp([]string{})
	require.NoError(t, err)
	require.NotRegexp(t, re, "a")
}
