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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/worker/scanner/executor"
)

const (
	executionsPageSize = 100

	taskStartupDelayRatio              = 100 * time.Millisecond
	taskStartupDelayRandomizationRatio = 1.0
)

type (
	// task is a runnable task that adheres to the executor.Task interface
	// for the scavenger, each of this task processes a single workflow mutableState
	task struct {
		shardID          int32
		executionManager persistence.ExecutionManager
		registry         namespace.Registry
		historyClient    historyservice.HistoryServiceClient
		adminClient      adminservice.AdminServiceClient
		metricsHandler   metrics.Handler
		logger           log.Logger
		scavenger        *Scavenger

		ctx                           context.Context
		rateLimiter                   quotas.RateLimiter
		executionDataDurationBuffer   dynamicconfig.DurationPropertyFn
		enableHistoryEventIDValidator dynamicconfig.BoolPropertyFn
		paginationToken               []byte
	}
)

// newTask returns a new instance of an executable task which will process a single mutableState
func newTask(
	ctx context.Context,
	shardID int32,
	executionManager persistence.ExecutionManager,
	registry namespace.Registry,
	historyClient historyservice.HistoryServiceClient,
	adminClient adminservice.AdminServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
	scavenger *Scavenger,
	rateLimiter quotas.RateLimiter,
	executionDataDurationBuffer dynamicconfig.DurationPropertyFn,
	enableHistoryEventIDValidator dynamicconfig.BoolPropertyFn,
) executor.Task {
	return &task{
		shardID:          shardID,
		executionManager: executionManager,
		registry:         registry,
		historyClient:    historyClient,
		adminClient:      adminClient,

		metricsHandler: metricsHandler.WithTags(metrics.OperationTag(metrics.ExecutionsScavengerScope)),
		logger:         logger,
		scavenger:      scavenger,

		ctx:                           ctx,
		rateLimiter:                   rateLimiter,
		executionDataDurationBuffer:   executionDataDurationBuffer,
		enableHistoryEventIDValidator: enableHistoryEventIDValidator,
	}
}

// Run runs the task
func (t *task) Run() executor.TaskStatus {
	time.Sleep(backoff.Jitter(
		taskStartupDelayRatio*time.Duration(t.scavenger.numHistoryShards),
		taskStartupDelayRandomizationRatio,
	))

	iter := collection.NewPagingIteratorWithToken(t.getPaginationFn(), t.paginationToken)
	var retryTask bool
	for iter.HasNext() {
		_ = t.rateLimiter.Wait(t.ctx)
		record, err := iter.Next()
		if err != nil {
			metrics.ScavengerValidationSkipsCount.With(t.metricsHandler).Record(1)
			// continue validation process and retry after all workflow records has been iterated.
			t.logger.Error("unable to paginate concrete execution", tag.ShardID(t.shardID), tag.Error(err))
			retryTask = true
		}

		mutableState := &MutableState{WorkflowMutableState: record}
		results := t.validate(mutableState)
		printValidationResult(
			mutableState,
			results,
			t.metricsHandler,
			t.logger,
		)
		err = t.handleFailures(mutableState, results)
		if err != nil {
			// continue validation process and retry after all workflow records has been iterated.
			executionInfo := mutableState.GetExecutionInfo()
			metrics.ScavengerValidationSkipsCount.With(t.metricsHandler).Record(1)
			t.logger.Error("unable to process failure result",
				tag.ShardID(t.shardID),
				tag.Error(err),
				tag.WorkflowNamespaceID(executionInfo.GetNamespaceId()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()))
			retryTask = true
		}
	}
	if retryTask {
		return executor.TaskStatusDefer
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

	if validationResults, err := NewMutableStateValidator(
		t.registry,
		t.executionDataDurationBuffer,
	).Validate(
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

	// Fail fast if the mutable is corrupted, no need to validate history.
	if len(results) > 0 {
		return results
	}

	if t.enableHistoryEventIDValidator() {
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

func (t *task) handleFailures(
	mutableState *MutableState,
	results []MutableStateValidationResult,
) error {
	for _, failure := range results {
		switch failure.failureType {
		case mutableStateRetentionFailureType:
			executionInfo := mutableState.GetExecutionInfo()
			runID := mutableState.GetExecutionState().GetRunId()
			ns, err := t.registry.GetNamespaceByID(namespace.ID(executionInfo.GetNamespaceId()))
			switch err.(type) {
			case *serviceerror.NotFound,
				*serviceerror.NamespaceNotFound:
				t.logger.Error("Garbage data in DB after namespace is deleted", tag.WorkflowNamespaceID(executionInfo.GetNamespaceId()))
				// We cannot do much in this case. It just ignores this error.
				return nil
			case nil:
				// continue to delete
			default:
				return err
			}

			_, err = t.adminClient.DeleteWorkflowExecution(t.ctx, &adminservice.DeleteWorkflowExecutionRequest{
				Namespace: ns.Name().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: executionInfo.GetWorkflowId(),
					RunId:      runID,
				},
			})
			switch err.(type) {
			case *serviceerror.NotFound,
				*serviceerror.NamespaceNotFound:
				return nil
			case nil:
				continue
			default:
				return err
			}
		default:
			// no-op
			continue
		}
	}
	return nil
}

func printValidationResult(
	mutableState *MutableState,
	results []MutableStateValidationResult,
	metricsHandler metrics.Handler,
	logger log.Logger,
) {
	metrics.ScavengerValidationRequestsCount.With(metricsHandler).Record(1)
	if len(results) == 0 {
		return
	}

	metrics.ScavengerValidationFailuresCount.With(metricsHandler).Record(1)
	for _, result := range results {
		metrics.ScavengerValidationFailuresCount.With(metricsHandler).Record(1, metrics.FailureTag(result.failureType))
		logger.Info(
			"validation failed for execution.",
			tag.WorkflowNamespaceID(mutableState.GetExecutionInfo().GetNamespaceId()),
			tag.WorkflowID(mutableState.GetExecutionInfo().GetWorkflowId()),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			tag.Value(result.failureDetails),
		)
	}
}
