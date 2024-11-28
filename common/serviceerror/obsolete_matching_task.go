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

package serviceerror

import (
	"go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ObsoleteMatchingTask happens when History determines a task that Matching wants to dispatch
	// is no longer valid. Some examples:
	// - WFT belongs to a sticky queue but the workflow is not on that sticky queue anymore.
	// - WFT belongs to the normal queue but no the workflow is on a sticky queue.
	// - WF or activity task wants to start but their schedule time directive deployment no longer
	//   matched the workflows effective deployment.
	// When Matching receives this error it can safely drop the task because History has already
	// scheduled new Matching tasks on the right task queue and deployment.
	ObsoleteMatchingTask struct {
		Message string
		st      *status.Status
	}
)

func NewObsoleteMatchingTask(msg string) error {
	return &ObsoleteMatchingTask{
		Message: msg,
	}
}

// Error returns string message.
func (e *ObsoleteMatchingTask) Error() string {
	return e.Message
}

func (e *ObsoleteMatchingTask) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetails.ObsoleteMatchingTaskFailure{},
	)
	return st
}

func newObsoleteMatchingTask(st *status.Status) error {
	return &ObsoleteMatchingTask{
		Message: st.Message(),
		st:      st,
	}
}
