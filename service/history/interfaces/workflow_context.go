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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_context_mock.go

package interfaces

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	// ReleaseWorkflowContextFunc must be called to release the workflow context.
	// Make sure not to access the mutable state or workflow context after releasing back to the cache.
	// If there is any error when using the mutable state (e.g. mutable state is mutated and dirty), call release with
	// the error so the in-memory copy will be thrown away.
	ReleaseWorkflowContextFunc func(err error)

	WorkflowContext interface {
		GetWorkflowKey() definition.WorkflowKey

		LoadMutableState(ctx context.Context, shardContext ShardContext) (MutableState, error)
		LoadExecutionStats(ctx context.Context, shardContext ShardContext) (*persistencespb.ExecutionStats, error)
		Clear()

		Lock(ctx context.Context, lockPriority locks.Priority) error
		Unlock()

		IsDirty() bool

		RefreshTasks(ctx context.Context, shardContext ShardContext) error

		ReapplyEvents(
			ctx context.Context,
			shardContext ShardContext,
			eventBatches []*persistence.WorkflowEvents,
		) error

		PersistWorkflowEvents(
			ctx context.Context,
			shardContext ShardContext,
			workflowEventsSlice ...*persistence.WorkflowEvents,
		) (int64, error)

		CreateWorkflowExecution(
			ctx context.Context,
			shardContext ShardContext,
			createMode persistence.CreateWorkflowMode,
			prevRunID string,
			prevLastWriteVersion int64,
			newMutableState MutableState,
			newWorkflow *persistence.WorkflowSnapshot,
			newWorkflowEvents []*persistence.WorkflowEvents,
		) error
		ConflictResolveWorkflowExecution(
			ctx context.Context,
			shardContext ShardContext,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetMutableState MutableState,
			newContext WorkflowContext,
			newMutableState MutableState,
			currentContext WorkflowContext,
			currentMutableState MutableState,
			resetWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
			currentTransactionPolicy *TransactionPolicy,
		) error
		UpdateWorkflowExecutionAsActive(
			ctx context.Context,
			shardContext ShardContext,
		) error
		UpdateWorkflowExecutionWithNewAsActive(
			ctx context.Context,
			shardContext ShardContext,
			newContext WorkflowContext,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionAsPassive(
			ctx context.Context,
			shardContext ShardContext,
		) error
		UpdateWorkflowExecutionWithNewAsPassive(
			ctx context.Context,
			shardContext ShardContext,
			newContext WorkflowContext,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionWithNew(
			ctx context.Context,
			shardContext ShardContext,
			updateMode persistence.UpdateWorkflowMode,
			newContext WorkflowContext,
			newMutableState MutableState,
			updateWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
		) error
		// SetWorkflowExecution is an alias to SubmitClosedWorkflowSnapshot with TransactionPolicyPassive.
		SetWorkflowExecution(
			ctx context.Context,
			shardContext ShardContext,
		) error
		// SubmitClosedWorkflowSnapshot closes the current mutable state transaction with the given
		// transactionPolicy and updates the workflow execution record in the DB. Does not check the "current"
		// run status for the execution.
		// Closes the transaction as snapshot, which errors out if there are any buffered events that need
		// flushing and generally does not expect new history events to be generated (expected for closed
		// workflows).
		// NOTE: in the future, we'd like to have the ability to close the transaction as mutation to avoid the
		// overhead of overwriting the entire DB record.
		SubmitClosedWorkflowSnapshot(
			ctx context.Context,
			shardContext ShardContext,
			transactionPolicy TransactionPolicy,
		) error
		// TODO (alex-update): move this from workflow context.
		UpdateRegistry(ctx context.Context) update.Registry
	}
)
