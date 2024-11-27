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

package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*SyncVersionedTransitionTask)(nil)

type (
	SyncVersionedTransitionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Priority            enumsspb.TaskPriority

		VersionedTransition *persistencespb.VersionedTransition
		FirstEventVersion   int64
		FirstEventID        int64
		NextEventID         int64
		NewRunID            string

		TaskEquivalents []Task
	}
)

func (a *SyncVersionedTransitionTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncVersionedTransitionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncVersionedTransitionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncVersionedTransitionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncVersionedTransitionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncVersionedTransitionTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncVersionedTransitionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION
}

func (a *SyncVersionedTransitionTask) String() string {
	return fmt.Sprintf("SyncVersionedTransitionTask{WorkflowKey: %v, TaskID: %v, Priority: %v, VersionedTransition: %v, FirstEventID: %v, FirstEventVersion: %v, NextEventID: %v, NewRunID: %v}",
		a.WorkflowKey, a.TaskID, a.Priority, a.VersionedTransition, a.FirstEventID, a.FirstEventVersion, a.NextEventID, a.NewRunID)
}
