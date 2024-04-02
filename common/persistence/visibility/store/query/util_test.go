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

package query

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseExecutionDurationStr(t *testing.T) {
	s := assert.New(t)

	testCases := []struct {
		input         string
		expectedValue time.Duration
		expectedErr   error
	}{
		{
			input:         "123",
			expectedValue: time.Duration(123),
		},
		{
			input:         "123s",
			expectedValue: 123 * time.Second,
		},
		{
			input:         "123m",
			expectedValue: 123 * time.Minute,
		},
		{
			input:         "123h",
			expectedValue: 123 * time.Hour,
		},
		{
			input:         "123d",
			expectedValue: 123 * 24 * time.Hour,
		},
		{
			input:         "01:02:03",
			expectedValue: 1*time.Hour + 2*time.Minute + 3*time.Second,
		},
		{
			input:       "01:60:03",
			expectedErr: errors.New("invalid duration"),
		},
		{
			input:       "123q",
			expectedErr: errors.New("invalid duration"),
		},
	}

	for _, tc := range testCases {
		got, err := ParseExecutionDurationStr(tc.input)
		if tc.expectedErr == nil {
			s.Equal(tc.expectedValue, got)
			s.NoError(err)
		} else {
			s.Error(err)
			s.Contains(err.Error(), tc.expectedErr.Error())
		}
	}
}
