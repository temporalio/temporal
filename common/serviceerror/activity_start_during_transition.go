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
	// ActivityStartDuringTransition when an activity start is rejected by History because the
	// workflow is in a transitioning between worker deployments.
	// When Matching sees this error it can safely drop the activity task because History will
	// reschedule the activity task after the transition is complete.
	ActivityStartDuringTransition struct {
		Message string
		st      *status.Status
	}
)

func NewActivityStartDuringTransition() error {
	return &ActivityStartDuringTransition{
		Message: "activity start attempted during deployment transition",
	}
}

// Error returns string message.
func (e *ActivityStartDuringTransition) Error() string {
	return e.Message
}

func (e *ActivityStartDuringTransition) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetails.ActivityStartDuringTransitionFailure{},
	)
	return st
}

func newActivityStartDuringTransition(st *status.Status) error {
	return &ActivityStartDuringTransition{
		Message: st.Message(),
		st:      st,
	}
}
