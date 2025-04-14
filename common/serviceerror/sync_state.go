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
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	// SyncState represents sync state error.
	SyncState struct {
		Message             string
		NamespaceId         string
		WorkflowId          string
		RunId               string
		VersionedTransition *persistencespb.VersionedTransition
		VersionHistories    *historyspb.VersionHistories
		st                  *status.Status
	}
)

// NewSyncState returns new SyncState error.
func NewSyncState(
	message string,
	namespaceId string,
	workflowId string,
	runId string,
	versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
) error {
	return &SyncState{
		Message:             message,
		NamespaceId:         namespaceId,
		WorkflowId:          workflowId,
		RunId:               runId,
		VersionedTransition: versionedTransition,
		VersionHistories:    versionHistories,
	}
}

// Error returns string message.
func (e *SyncState) Error() string {
	return e.Message
}

func (e *SyncState) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.SyncStateFailure{
			NamespaceId:         e.NamespaceId,
			WorkflowId:          e.WorkflowId,
			RunId:               e.RunId,
			VersionedTransition: e.VersionedTransition,
			VersionHistories:    e.VersionHistories,
		},
	)
	return st
}

func (e *SyncState) Equal(err *SyncState) bool {
	return e.NamespaceId == err.NamespaceId &&
		e.WorkflowId == err.WorkflowId &&
		e.RunId == err.RunId &&
		proto.Equal(e.VersionedTransition, err.VersionedTransition) &&
		proto.Equal(e.VersionHistories, err.VersionHistories)
}

func newSyncState(
	st *status.Status,
	errDetails *errordetailsspb.SyncStateFailure,
) error {
	return &SyncState{
		Message:             st.Message(),
		NamespaceId:         errDetails.GetNamespaceId(),
		WorkflowId:          errDetails.GetWorkflowId(),
		RunId:               errDetails.GetRunId(),
		VersionedTransition: errDetails.GetVersionedTransition(),
		VersionHistories:    errDetails.GetVersionHistories(),
		st:                  st,
	}
}
