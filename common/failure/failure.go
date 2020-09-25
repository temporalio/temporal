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
	if f == nil {
		return nil
	}

	newFailure := &failurepb.Failure{
		Source: f.Source,
	}

	// Keep failure info for ApplicationFailureInfo and for ServerFailureInfo to persist NonRetryable flag.
	if f.GetApplicationFailureInfo() != nil {
		newFailure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: f.GetApplicationFailureInfo().GetNonRetryable(),
			Type:         f.GetApplicationFailureInfo().GetType(),
		}}
	}

	if f.GetServerFailureInfo() != nil {
		newFailure.FailureInfo = &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: f.GetServerFailureInfo().GetNonRetryable(),
		}}
	}

	if len(f.Message) > maxSize {
		newFailure.Message = f.Message[:maxSize]
		return newFailure
	}
	newFailure.Message = f.Message
	maxSize -= len(newFailure.Message)

	if len(f.StackTrace) > maxSize {
		newFailure.StackTrace = f.StackTrace[:maxSize]
		return newFailure
	}
	newFailure.StackTrace = f.StackTrace
	maxSize -= len(newFailure.StackTrace)

	newFailure.Cause = Truncate(f.Cause, maxSize)

	return newFailure
}
