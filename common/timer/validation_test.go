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

package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestValidateAndCapTimer(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                  string
		timerDuration         *durationpb.Duration
		expectedErr           error
		expectedTimerDuration *durationpb.Duration
	}{
		{
			name:                  "nil timer duration",
			timerDuration:         nil,
			expectedErr:           nil,
			expectedTimerDuration: nil,
		},
		{
			name:                  "negative timer duration",
			timerDuration:         durationpb.New(-time.Minute),
			expectedErr:           errNegativeDuration,
			expectedTimerDuration: nil,
		},
		{
			name:                  "zero timer duration",
			timerDuration:         durationpb.New(0),
			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(0),
		},
		{
			name:          "cap timer duration",
			timerDuration: durationpb.New(200 * 365 * 24 * time.Hour),

			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(MaxAllowedTimer),
		},
		{
			name:                  "valid timer duration",
			timerDuration:         durationpb.New(time.Hour),
			expectedErr:           nil,
			expectedTimerDuration: durationpb.New(time.Hour),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actualErr := ValidateAndCapTimer(tc.timerDuration)

			assert.Equal(t, tc.expectedErr, actualErr)
			if tc.expectedErr == nil {
				assert.Equal(t, tc.expectedTimerDuration, tc.timerDuration)
			}
		})
	}
}
