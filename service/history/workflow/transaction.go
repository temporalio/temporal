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

package workflow

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transaction_mock.go
type (
	Transaction interface {
		CreateWorkflowExecution(
			ctx context.Context,
			createMode persistence.CreateWorkflowMode,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			clusterName string,
		) (int64, error)

		ConflictResolveWorkflowExecution(
			ctx context.Context,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetWorkflowSnapshot *persistence.WorkflowSnapshot,
			resetWorkflowEventsSeq []*persistence.WorkflowEvents,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			currentWorkflowMutation *persistence.WorkflowMutation,
			currentWorkflowEventsSeq []*persistence.WorkflowEvents,
			clusterName string,
		) (int64, int64, int64, error)

		UpdateWorkflowExecution(
			ctx context.Context,
			updateMode persistence.UpdateWorkflowMode,
			currentWorkflowMutation *persistence.WorkflowMutation,
			currentWorkflowEventsSeq []*persistence.WorkflowEvents,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			clusterName string,
		) (int64, int64, error)

		SetWorkflowExecution(
			ctx context.Context,
			workflowSnapshot *persistence.WorkflowSnapshot,
			clusterName string,
		) error
	}
)
