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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateUTF8(t *testing.T) {
	s := "hello \u2603!!!"
	assert.Equal(t, 12, len(s))
	assert.Equal(t, "hello \u2603!!!", TruncateUTF8(s, 20))
	assert.Equal(t, "hello \u2603!!!", TruncateUTF8(s, 12))
	assert.Equal(t, "hello \u2603!!", TruncateUTF8(s, 11))
	assert.Equal(t, "hello \u2603!", TruncateUTF8(s, 10))
	assert.Equal(t, "hello \u2603", TruncateUTF8(s, 9))
	assert.Equal(t, "hello ", TruncateUTF8(s, 8))
	assert.Equal(t, "hello ", TruncateUTF8(s, 7))
	assert.Equal(t, "hello ", TruncateUTF8(s, 6))
	assert.Equal(t, "hello", TruncateUTF8(s, 5))
	assert.Equal(t, "", TruncateUTF8(s, 0))
	assert.Equal(t, "", TruncateUTF8(s, -3))
}
