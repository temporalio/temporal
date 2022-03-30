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

package executions

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/worker/scanner/executor"
)

const (
	executionsPageSize = 100

	ratePerShard = 1
	rateOverall  = 10

	taskStartupDelayRatio              = 100 * time.Millisecond
	taskStartupDelayRandomizationRatio = 1.0
)

type (
	// task is a runnable task that adheres to the executor.Task interface
	// for the scavenger, each of this task processes a single workflow mutableState
	task struct {
		shardID          int32
		executionManager persistence.ExecutionManager
		metrics          metrics.Client
		logger           log.Logger
		scavenger        *Scavenger

		ctx             context.Context
		rateLimiter     quotas.RateLimiter
		paginationToken []byte
	}
)

// newTask returns a new instance of an executable task which will process a single mutableState
func newTask(
	shardID int32,
	executionManager persistence.ExecutionManager,
	metrics metrics.Client,
	logger log.Logger,
	scavenger *Scavenger,
	rateLimiter quotas.RateLimiter,
) executor.Task {
	return &task{
		shardID:          shardID,
		executionManager: executionManager,

		metrics:   metrics,
		logger:    logger,
		scavenger: scavenger,

		ctx:         context.Background(), // TODO: use context from ExecutionsScavengerActivity
		rateLimiter: rateLimiter,
	}
}

// Run runs the task
func (t *task) Run() executor.TaskStatus {
	time.Sleep(backoff.JitDuration(
		taskStartupDelayRatio*time.Duration(t.scavenger.numHistoryShards),
		taskStartupDelayRandomizationRatio,
	))

	iter := collection.NewPagingIteratorWithToken(t.getPaginationFn(), t.paginationToken)
	for iter.HasNext() {
		// ctx is background, do not expect interruption
		_ = t.rateLimiter.Wait(t.ctx)
		record, err := iter.Next()
		if err != nil {
			t.logger.Error("unable to paginate concrete execution", tag.ShardID(t.shardID), tag.Error(err))
			return executor.TaskStatusDefer
		}

		mutableState := &MutableState{WorkflowMutableState: record}
		printValidationResult(
			mutableState,
			t.validate(mutableState),
			t.metrics,
			t.logger,
		)
	}
	return executor.TaskStatusDone
}

func (t *task) validate(
	mutableState *MutableState,
) []MutableStateValidationResult {

	var results []MutableStateValidationResult
	t.logger.Debug("validating mutable state",
		tag.ShardID(t.shardID),
		tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().GetNamespaceId()),
		tag.WorkflowID(mutableState.GetExecutionInfo().GetWorkflowId()),
		tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
	)

	if validationResults, err := NewMutableStateIDValidator().Validate(
		t.ctx,
		mutableState,
	); err != nil {
		t.logger.Error("unable to validate mutable state ID",
			tag.ShardID(t.shardID),
			tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().GetNamespaceId()),
			tag.WorkflowID(mutableState.GetExecutionInfo().GetWorkflowId()),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			tag.Error(err),
		)
	} else {
		results = append(results, validationResults...)
	}

	if validationResults, err := NewHistoryEventIDValidator(
		t.shardID,
		t.executionManager,
	).Validate(t.ctx, mutableState); err != nil {
		t.logger.Error("unable to validate history event ID being contiguous",
			tag.ShardID(t.shardID),
			tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().GetNamespaceId()),
			tag.WorkflowID(mutableState.GetExecutionInfo().GetWorkflowId()),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			tag.Error(err),
		)
	} else {
		results = append(results, validationResults...)
	}

	return results
}

func (t *task) getPaginationFn() collection.PaginationFn[*persistencespb.WorkflowMutableState] {
	return func(paginationToken []byte) ([]*persistencespb.WorkflowMutableState, []byte, error) {
		req := &persistence.ListConcreteExecutionsRequest{
			ShardID:   t.shardID,
			PageSize:  executionsPageSize,
			PageToken: paginationToken,
		}
		resp, err := t.executionManager.ListConcreteExecutions(t.ctx, req)
		if err != nil {
			return nil, nil, err
		}
		paginateItems := resp.States
		t.paginationToken = resp.PageToken
		return paginateItems, resp.PageToken, nil
	}
}

func printValidationResult(
	mutableState *MutableState,
	results []MutableStateValidationResult,
	metricClient metrics.Client,
	logger log.Logger,
) {

	metricsScope := metricClient.Scope(metrics.ExecutionsScavengerScope).Tagged(
		metrics.FailureTag(""),
	)
	metricsScope.IncCounter(metrics.ScavengerValidationRequestsCount)
	if len(results) == 0 {
		return
	}

	metricsScope.IncCounter(metrics.ScavengerValidationFailuresCount)
	for _, result := range results {
		metricsScope.Tagged(
			metrics.FailureTag(result.failureType),
		).IncCounter(metrics.ScavengerValidationFailuresCount)

		logger.Error(
			"validation failed for execution.",
			tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().GetNamespaceId()),
			tag.WorkflowID(mutableState.GetExecutionInfo().GetWorkflowId()),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			tag.Value(result.failureDetails),
		)
	}
}
