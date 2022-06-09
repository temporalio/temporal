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
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/api/errordetails/v1"
)

// FromStatus converts gogo gRPC status to service error.
func FromStatus(st *status.Status) error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}

	errDetails := extractErrorDetails(st)

	switch st.Code() {
	case codes.InvalidArgument:
		switch errDetails := errDetails.(type) {
		case *errordetails.CurrentBranchChangedFailure:
			return newCurrentBranchChanged(st, errDetails)
		}
	case codes.AlreadyExists:
		switch errDetails.(type) {
		case *errordetails.TaskAlreadyStartedFailure:
			return newTaskAlreadyStarted(st)
		}
	case codes.Aborted:
		switch errDetails := errDetails.(type) {
		case *errordetails.ShardOwnershipLostFailure:
			return newShardOwnershipLost(st, errDetails)
		case *errordetails.RetryReplicationFailure:
			return newRetryReplication(st, errDetails)
		}
	case codes.Unavailable:
		switch errDetails.(type) {
		case *errordetails.StickyWorkerUnavailableFailure:
			return newStickyWorkerUnavailable(st)
		}
	}

	return serviceerror.FromStatus(st)
}

func extractErrorDetails(st *status.Status) interface{} {
	details := st.Details()
	if len(details) > 0 {
		return details[0]
	}

	return nil
}
