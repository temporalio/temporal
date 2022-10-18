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

package definition

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination event_notifier_mock.go

type (
	Notifier interface {
		common.Daemon
		NotifyNewHistoryEvent(event *Notification)
		WatchHistoryEvent(identifier definition.WorkflowKey) (string, chan *Notification, error)
		UnwatchHistoryEvent(identifier definition.WorkflowKey, subscriberID string) error
	}

	Notification struct {
		ID                     definition.WorkflowKey
		LastFirstEventID       int64
		LastFirstEventTxnID    int64
		NextEventID            int64
		PreviousStartedEventID int64
		Timestamp              time.Time
		CurrentBranchToken     []byte
		WorkflowState          enumsspb.WorkflowExecutionState
		WorkflowStatus         enumspb.WorkflowExecutionStatus
	}
)

func NewNotification(
	namespaceID string,
	workflowExecution *commonpb.WorkflowExecution,
	lastFirstEventID int64,
	lastFirstEventTxnID int64,
	nextEventID int64,
	previousStartedEventID int64,
	currentBranchToken []byte,
	workflowState enumsspb.WorkflowExecutionState,
	workflowStatus enumspb.WorkflowExecutionStatus,
) *Notification {

	return &Notification{
		ID: definition.NewWorkflowKey(
			namespaceID,
			workflowExecution.GetWorkflowId(),
			workflowExecution.GetRunId(),
		),
		LastFirstEventID:       lastFirstEventID,
		LastFirstEventTxnID:    lastFirstEventTxnID,
		NextEventID:            nextEventID,
		PreviousStartedEventID: previousStartedEventID,
		CurrentBranchToken:     currentBranchToken,
		WorkflowState:          workflowState,
		WorkflowStatus:         workflowStatus,
	}
}
