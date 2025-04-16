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
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// CurrentBranchChanged represents current branch changed error.
	CurrentBranchChanged struct {
		Message                    string
		CurrentBranchToken         []byte
		RequestBranchToken         []byte
		CurrentVersionedTransition *persistencespb.VersionedTransition
		RequestVersionedTransition *persistencespb.VersionedTransition
		st                         *status.Status
	}
)

// NewCurrentBranchChanged returns new CurrentBranchChanged error.
// TODO: Update CurrentBranchChanged with event id and event version. Do not use branch token bytes as branch identity.
func NewCurrentBranchChanged(currentBranchToken, requestBranchToken []byte,
	currentVersionedTransition, requestVersionedTransition *persistencespb.VersionedTransition) error {
	return &CurrentBranchChanged{
		Message:                    "Current and request branch tokens, or current and request versioned transitions, don't match.",
		CurrentBranchToken:         currentBranchToken,
		RequestBranchToken:         requestBranchToken,
		CurrentVersionedTransition: currentVersionedTransition,
		RequestVersionedTransition: requestVersionedTransition,
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
		&errordetailsspb.CurrentBranchChangedFailure{
			CurrentBranchToken:         e.CurrentBranchToken,
			RequestBranchToken:         e.RequestBranchToken,
			CurrentVersionedTransition: e.CurrentVersionedTransition,
			RequestVersionedTransition: e.RequestVersionedTransition,
		},
	)
	return st
}

func newCurrentBranchChanged(st *status.Status, errDetails *errordetailsspb.CurrentBranchChangedFailure) error {
	return &CurrentBranchChanged{
		Message:                    st.Message(),
		CurrentBranchToken:         errDetails.GetCurrentBranchToken(),
		RequestBranchToken:         errDetails.GetRequestBranchToken(),
		CurrentVersionedTransition: errDetails.GetCurrentVersionedTransition(),
		RequestVersionedTransition: errDetails.GetRequestVersionedTransition(),
		st:                         st,
	}
}
