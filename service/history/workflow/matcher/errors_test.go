// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matcher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewMatcherError ensures that NewMatcherError formats its output correctly
// and returns an object of type *Error.
func TestNewMatcherError(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		args        []interface{}
		expectedMsg string
	}{
		{
			name:        "simple message",
			format:      "a simple message",
			args:        nil,
			expectedMsg: "a simple message",
		},
		{
			name:        "formatted message",
			format:      "value = %d, str = %s",
			args:        []interface{}{42, "hello"},
			expectedMsg: "value = 42, str = hello",
		},
		{
			name:        "no format specifiers",
			format:      "no format specifiers",
			args:        nil,
			expectedMsg: "no format specifiers",
		},
		{
			name:        "multiple format specifiers",
			format:      "%s: %s -> %d",
			args:        []interface{}{"error", "some detail", 123},
			expectedMsg: "error: some detail -> 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var matchErr *Error
			err := NewMatcherError(tt.format, tt.args...)

			assert.Error(t, err)
			assert.ErrorAs(t, err, &matchErr)
			assert.Equal(t, tt.expectedMsg, matchErr.Error())
		})
	}
}

// TestWrapMatcherError ensures that wrapMatcherError behaves correctly under multiple scenarios.
func TestWrapMatcherError(t *testing.T) {
	t.Run("err is nil", func(t *testing.T) {
		var err error
		got := wrapMatcherError("some message", err)
		assert.Nil(t, got)
	})

	t.Run("err is non-matcher error", func(t *testing.T) {
		originalErr := errors.New("some error")
		got := wrapMatcherError("prefix", originalErr)
		assert.Equal(t, originalErr, got)
	})

	t.Run("err is matcher error", func(t *testing.T) {
		originalErr := &Error{message: "matcher error"} // Adjust field name if needed
		got := wrapMatcherError("prefix", originalErr)
		assert.NotNil(t, got)

		var matcherErr Error
		assert.ErrorAs(t, got, &matcherErr)

		// Check if message is wrapped properly
		expectedMsg := "prefix: matcher error"
		assert.Equal(t, expectedMsg, matcherErr.Error())
	})
}
