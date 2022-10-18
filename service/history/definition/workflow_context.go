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
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
)

const (
	CallerTypeAPI  CallerType = 0
	CallerTypeTask CallerType = 1
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_context_mock.go

type (
	CallerType int

	WorkflowContext interface {
		GetWorkflowKey() definition.WorkflowKey

		LoadMutableState(ctx context.Context) (MutableState, error)
		LoadExecutionStats(ctx context.Context) (*persistencespb.ExecutionStats, error)
		Clear()

		Lock(ctx context.Context, caller CallerType) error
		Unlock(caller CallerType)

		GetHistorySize() int64
		SetHistorySize(size int64)

		ReapplyEvents(
			eventBatches []*persistence.WorkflowEvents,
		) error

		PersistWorkflowEvents(
			ctx context.Context,
			workflowEvents *persistence.WorkflowEvents,
		) (int64, error)

		CreateWorkflowExecution(
			ctx context.Context,
			now time.Time,
			createMode persistence.CreateWorkflowMode,
			prevRunID string,
			prevLastWriteVersion int64,
			newMutableState MutableState,
			newWorkflow *persistence.WorkflowSnapshot,
			newWorkflowEvents []*persistence.WorkflowEvents,
		) error
		ConflictResolveWorkflowExecution(
			ctx context.Context,
			now time.Time,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetMutableState MutableState,
			newContext WorkflowContext,
			newMutableState MutableState,
			currentContext WorkflowContext,
			currentMutableState MutableState,
			currentTransactionPolicy *TransactionPolicy,
		) error
		UpdateWorkflowExecutionAsActive(
			ctx context.Context,
			now time.Time,
		) error
		UpdateWorkflowExecutionWithNewAsActive(
			ctx context.Context,
			now time.Time,
			newContext WorkflowContext,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionAsPassive(
			ctx context.Context,
			now time.Time,
		) error
		UpdateWorkflowExecutionWithNewAsPassive(
			ctx context.Context,
			now time.Time,
			newContext WorkflowContext,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionWithNew(
			ctx context.Context,
			now time.Time,
			updateMode persistence.UpdateWorkflowMode,
			newContext WorkflowContext,
			newMutableState MutableState,
			currentWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
		) error
		SetWorkflowExecution(
			ctx context.Context,
			now time.Time,
		) error
	}
)
