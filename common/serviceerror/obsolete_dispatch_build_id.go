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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.temporal.io/server/api/errordetails/v1"
)

type (
	// ObsoleteDispatchBuildId happens when matching wants to dispatch task to an obsolete build ID. This is expected to
	// happen when a workflow has concurrent tasks (and in some other edge cases) and redirect rules apply to the WF.
	// In that case, tasks already scheduled but not started will become invalid and History reschedules them in the
	// new build ID. Matching will still try dispatching the old tasks but it will face this error.
	// Matching can safely drop tasks which face this error.
	ObsoleteDispatchBuildId struct {
		Message string
		st      *status.Status
	}
)

func NewObsoleteDispatchBuildId() error {
	return &ObsoleteDispatchBuildId{
		Message: "dispatch build ID is not the workflow's current build ID",
	}
}

// Error returns string message.
func (e *ObsoleteDispatchBuildId) Error() string {
	return e.Message
}

func (e *ObsoleteDispatchBuildId) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetails.ObsoleteDispatchBuildIdFailure{},
	)
	return st
}

func newObsoleteDispatchBuildId(st *status.Status) error {
	return &ObsoleteDispatchBuildId{
		Message: st.Message(),
		st:      st,
	}
}
