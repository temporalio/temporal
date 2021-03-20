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
	// RetryReplication represents retry task v2 error.
	RetryReplication struct {
		Message           string
		NamespaceId       string
		WorkflowId        string
		RunId             string
		StartEventId      int64
		StartEventVersion int64
		EndEventId        int64
		EndEventVersion   int64
		st                *status.Status
	}
)

// NewRetryReplication returns new RetryReplication error.
func NewRetryReplication(
	message string,
	namespaceId string,
	workflowId string,
	runId string,
	startEventId int64,
	startEventVersion int64,
	endEventId int64,
	endEventVersion int64,
) error {
	return &RetryReplication{
		Message:           message,
		NamespaceId:       namespaceId,
		WorkflowId:        workflowId,
		RunId:             runId,
		StartEventId:      startEventId,
		StartEventVersion: startEventVersion,
		EndEventId:        endEventId,
		EndEventVersion:   endEventVersion,
	}
}

// Error returns string message.
func (e *RetryReplication) Error() string {
	return e.Message
}

func (e *RetryReplication) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetails.RetryReplicationFailure{
			NamespaceId:       e.NamespaceId,
			WorkflowId:        e.WorkflowId,
			RunId:             e.RunId,
			StartEventId:      e.StartEventId,
			StartEventVersion: e.StartEventVersion,
			EndEventId:        e.EndEventId,
			EndEventVersion:   e.EndEventVersion,
		},
	)
	return st
}

func newRetryReplication(
	st *status.Status,
	errDetails *errordetails.RetryReplicationFailure,
) error {
	return &RetryReplication{
		Message:           st.Message(),
		NamespaceId:       errDetails.GetNamespaceId(),
		WorkflowId:        errDetails.GetWorkflowId(),
		RunId:             errDetails.GetRunId(),
		StartEventId:      errDetails.GetStartEventId(),
		StartEventVersion: errDetails.GetStartEventVersion(),
		EndEventId:        errDetails.GetEndEventId(),
		EndEventVersion:   errDetails.GetEndEventVersion(),
		st:                st,
	}
}
