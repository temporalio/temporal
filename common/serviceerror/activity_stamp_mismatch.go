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
	"fmt"

	"go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ActivityStampMismatch represents actity related task that has diffent stamp.
	ActivityStampMismatch struct {
		Message string
		st      *status.Status
	}
)

// NewActivityStampMismatch returns new ActivityStampMismatch error.
func NewActivityStampMismatch(activityType string, oldStamp int32, newStamp int32) error {
	return &ActivityStampMismatch{
		Message: fmt.Sprintf("%s activity has stamp mismatch. Old stamp: %d, new stamp: %d.", activityType, oldStamp, newStamp),
	}
}

// Error returns string message.
func (e *ActivityStampMismatch) Error() string {
	return e.Message
}

func (e *ActivityStampMismatch) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.NotFound, e.Message)
	st, _ = st.WithDetails(
		&errordetails.ActivityStampMismatchFailure{},
	)
	return st
}

func newActivityStampMismatch(st *status.Status) error {
	return &TaskAlreadyStarted{
		Message: st.Message(),
		st:      st,
	}
}
