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

package failure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	failurepb "go.temporal.io/api/failure/v1"
)

func TestTruncate(t *testing.T) {
	assert := assert.New(t)

	f := &failurepb.Failure{
		Source:  "GoSDK",
		Message: "very long message",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: true,
		}},
	}

	maxSize := 18
	newFailure := Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Len(newFailure.Message, 5)
	assert.Equal(f.Source, newFailure.Source)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	f.StackTrace = "some stack trace"
	maxSize = len(f.Message) + len(f.Source) + 19
	newFailure = Truncate(f, maxSize)
	assert.LessOrEqual(newFailure.Size(), maxSize)
	assert.Equal(f.Message, newFailure.Message)
	assert.Equal(f.Source, newFailure.Source)
	assert.Equal("some st", newFailure.StackTrace)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	newFailure = Truncate(f, 1000)
	assert.Equal(f.Message, newFailure.Message)
	assert.Equal(f.Source, newFailure.Source)
	assert.Equal(f.StackTrace, newFailure.StackTrace)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())
}

func TestTruncateUTF8(t *testing.T) {
	f := &failurepb.Failure{
		Source:  "test",
		Message: "\u2299 very \u229a long \u25cd message",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: true,
		}},
	}
	assert.Equal(t, "\u2299 very \u229a", Truncate(f, 24).Message)
	assert.Equal(t, "\u2299 very ", Truncate(f, 23).Message)
}

func TestTruncateDepth(t *testing.T) {
	f := &failurepb.Failure{
		Message: "level 1",
		Cause: &failurepb.Failure{
			Message: "level 2",
			Cause: &failurepb.Failure{
				Message: "level 3",
				Cause: &failurepb.Failure{
					Message: "level 4",
					Cause: &failurepb.Failure{
						Message: "level 5",
					},
				},
			},
		},
	}

	trunc := TruncateWithDepth(f, 1000, 3)
	assert.Nil(t, trunc.Cause.Cause.Cause.Cause)

	trunc = TruncateWithDepth(f, 33, 100)
	assert.LessOrEqual(t, trunc.Size(), 33)
	assert.Nil(t, trunc.Cause.Cause.Cause)
	assert.Equal(t, "lev", trunc.Cause.Cause.Message)

	trunc = TruncateWithDepth(f, 30, 100)
	assert.LessOrEqual(t, trunc.Size(), 30)
	assert.Nil(t, trunc.Cause.Cause)
}
