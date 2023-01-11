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

package testutils

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRepoRootDirectory(t *testing.T) {
	t.Run("env var set", func(t *testing.T) {
		directory := GetRepoRootDirectory(WithGetenv(func(s string) string {
			assert.Equal(t, "TEMPORAL_ROOT", s)
			return "/tmp/temporal"
		}))
		assert.Equal(t, "/tmp/temporal", directory)
	})

	t.Run("env var not set", func(t *testing.T) {
		directory := GetRepoRootDirectory(WithGetenv(func(s string) string {
			return ""
		}))
		assert.True(
			t,
			strings.HasSuffix(directory, "temporal") || strings.HasSuffix(directory, "temporal/"),
			directory,
		)
	})
}
