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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/util"
)

const (
	failureSourceServer = "Server"
)

func NewServerFailure(message string, nonRetryable bool) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: nonRetryable,
		}},
	}

	return f
}

func NewResetWorkflowFailure(message string, lastHeartbeatDetails *commonpb.Payloads) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{
			LastHeartbeatDetails: lastHeartbeatDetails,
		}},
	}

	return f
}

func NewTimeoutFailure(message string, timeoutType enumspb.TimeoutType) *failurepb.Failure {
	f := &failurepb.Failure{
		Message: message,
		Source:  failureSourceServer,
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: timeoutType,
		}},
	}

	return f
}

func Truncate(f *failurepb.Failure, maxSize int) *failurepb.Failure {
	return TruncateWithDepth(f, maxSize, 20)
}

func TruncateWithDepth(f *failurepb.Failure, maxSize, maxDepth int) *failurepb.Failure {
	if f == nil {
		return nil
	}

	// note that bytes are given to earlier calls first, so call in order of importance
	trunc := func(s string) string {
		s = util.TruncateUTF8(s, maxSize)
		maxSize -= len(s)
		if s != "" {
			maxSize -= 4 // account for proto overhead
		}
		return s
	}

	newFailure := &failurepb.Failure{}

	// Keep failure info for ApplicationFailureInfo and for ServerFailureInfo to persist NonRetryable flag.
	if i := f.GetApplicationFailureInfo(); i != nil {
		newFailure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: i.NonRetryable,
			Type:         trunc(i.Type),
		}}
		maxSize -= 8 // account for proto overhead
	} else if i := f.GetServerFailureInfo(); i != nil {
		newFailure.FailureInfo = &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: i.NonRetryable,
		}}
		maxSize -= 4 // account for proto overhead
	}

	newFailure.Source = trunc(f.Source)
	newFailure.Message = trunc(f.Message)
	newFailure.StackTrace = trunc(f.StackTrace)
	if f.Cause != nil && maxSize > 4 && maxDepth > 0 {
		newFailure.Cause = TruncateWithDepth(f.Cause, maxSize-4, maxDepth-1)
	}

	return newFailure
}
