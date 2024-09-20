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
)

func TestSchemaDirs(t *testing.T) {
	dirs := PathsByDir("cassandra")
	requireContains(t, []string{
		"cassandra/temporal",
	}, dirs)

	dirs = PathsByDir("mysql")
	requireContains(t, []string{
		"mysql/v8/temporal",
		"mysql/v8/visibility",
	}, dirs)

	dirs = PathsByDir("postgresql")
	requireContains(t, []string{
		"postgresql/v12/temporal",
		"postgresql/v12/visibility",
	}, dirs)
}

func requireContains(t *testing.T, expected []string, actual []string) {
	for _, v := range expected {
		require.Contains(t, actual, v)
	}
}
