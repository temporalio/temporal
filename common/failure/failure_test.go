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

	newFailure := Truncate(f, 4)
	assert.Len(newFailure.GetMessage(), 4)
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	f.StackTrace = "some stack trace"
	newFailure = Truncate(f, len(f.GetMessage())+4)
	assert.Equal(f.GetMessage(), newFailure.GetMessage())
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.Len(newFailure.GetStackTrace(), 4)
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())

	newFailure = Truncate(f, 1000)
	assert.Equal(f.GetMessage(), newFailure.GetMessage())
	assert.Equal(f.GetSource(), newFailure.GetSource())
	assert.Equal(f.GetStackTrace(), newFailure.GetStackTrace())
	assert.NotNil(newFailure.GetServerFailureInfo())
	assert.True(newFailure.GetServerFailureInfo().GetNonRetryable())
}
