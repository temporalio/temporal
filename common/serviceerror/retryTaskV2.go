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
	// RetryTaskV2 represents retry task v2 error.
	RetryTaskV2 struct {
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

// NewRetryTaskV2 returns new RetryTaskV2 error.
func NewRetryTaskV2(message, namespaceId, workflowId, runId string, startEventId, startEventVersion, endEventId, endEventVersion int64) *RetryTaskV2 {
	return &RetryTaskV2{
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
func (e *RetryTaskV2) Error() string {
	return e.Message
}

func (e *RetryTaskV2) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetails.RetryTaskV2Failure{
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

func newRetryTaskV2(st *status.Status, errDetails *errordetails.RetryTaskV2Failure) *RetryTaskV2 {
	return &RetryTaskV2{
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
