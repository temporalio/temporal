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

package serviceerror

import (
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/api/errordetails/v1"
)

type (
	// CurrentBranchChanged represents current branch changed error.
	CurrentBranchChanged struct {
		Message            string
		CurrentBranchToken []byte
		RequestBranchToken []byte
		st                 *status.Status
	}
)

// NewCurrentBranchChanged returns new CurrentBranchChanged error.
func NewCurrentBranchChanged(currentBranchToken, requestBranchToken []byte) error {
	return &CurrentBranchChanged{
		Message:            "Current branch token and request branch token doesn't match.",
		CurrentBranchToken: currentBranchToken,
		RequestBranchToken: requestBranchToken,
	}
}

// Error returns string message.
func (e *CurrentBranchChanged) Error() string {
	return e.Message
}

func (e *CurrentBranchChanged) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.InvalidArgument, e.Message)
	st, _ = st.WithDetails(
		&errordetails.CurrentBranchChangedFailure{
			CurrentBranchToken: e.CurrentBranchToken,
			RequestBranchToken: e.RequestBranchToken,
		},
	)
	return st
}

func newCurrentBranchChanged(st *status.Status, errDetails *errordetails.CurrentBranchChangedFailure) error {
	return &CurrentBranchChanged{
		Message:            st.Message(),
		CurrentBranchToken: errDetails.GetCurrentBranchToken(),
		RequestBranchToken: errDetails.GetRequestBranchToken(),
		st:                 st,
	}
}
