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

package history

import (
	"context"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
)

type (
	// ScavengerHeartbeatDetails is the heartbeat detail for HistoryScavengerActivity
	ScavengerHeartbeatDetails struct {
		SuccessCount int
		ErrorCount   int
		SkipCount    int
		CurrentPage  int

		NextPageToken []byte
	}

	// Scavenger is the type that holds the state for history scavenger daemon
	Scavenger struct {
		numShards      int32
		db             persistence.ExecutionManager
		client         historyservice.HistoryServiceClient
		adminClient    adminservice.AdminServiceClient
		registry       namespace.Registry
		rateLimiter    quotas.RateLimiter
		metricsHandler metrics.Handler
		logger         log.Logger
		isInTest       bool
		// only clean up history branches that older than this age
		// Our history archiver delete mutable state, and then upload history to blob store and then delete history.
		historyDataMinAge           dynamicconfig.DurationPropertyFn
		executionDataDurationBuffer dynamicconfig.DurationPropertyFn
		enableRetentionVerification dynamicconfig.BoolPropertyFn

		sync.WaitGroup
		sync.Mutex
		hbd ScavengerHeartbeatDetails
	}

	taskDetail struct {
		shardID     int32
		namespaceID string
		workflowID  string
		runID       string
		branchToken []byte
	}
)

const (
	pageSize  = 100
	numWorker = 10
)

// NewScavenger returns an instance of history scavenger daemon
// The Scavenger can be started by calling the Run() method on the
// returned object. Calling the Run() method will result in one
// complete iteration over all of the history branches in the system. For
// each branch, the scavenger will attempt
//   - describe the corresponding workflow execution
//   - deletion of history itself, if there are no workflow execution
func NewScavenger(
	numShards int32,
	db persistence.ExecutionManager,
	rps int,
	client historyservice.HistoryServiceClient,
	adminClient adminservice.AdminServiceClient,
	registry namespace.Registry,
	hbd ScavengerHeartbeatDetails,
	historyDataMinAge dynamicconfig.DurationPropertyFn,
	executionDataDurationBuffer dynamicconfig.DurationPropertyFn,
	enableRetentionVerification dynamicconfig.BoolPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *Scavenger {

	return &Scavenger{
		numShards:   numShards,
		db:          db,
		client:      client,
		adminClient: adminClient,
		registry:    registry,
		rateLimiter: quotas.NewDefaultOutgoingRateLimiter(
			func() float64 { return float64(rps) },
		),
		historyDataMinAge:           historyDataMinAge,
		executionDataDurationBuffer: executionDataDurationBuffer,
		enableRetentionVerification: enableRetentionVerification,
		metricsHandler:              metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryScavengerScope)),
		logger:                      logger,

		hbd: hbd,
	}
}

// Run runs the scavenger
func (s *Scavenger) Run(ctx context.Context) (ScavengerHeartbeatDetails, error) {
	reqCh := make(chan taskDetail, pageSize)

	go s.loadTasks(ctx, reqCh)
	for i := 0; i < numWorker; i++ {
		s.WaitGroup.Add(1)
		go s.taskWorker(ctx, reqCh)
	}

	s.WaitGroup.Wait()

	s.Lock()
	defer s.Unlock()
	return s.hbd, nil
}

func (s *Scavenger) loadTasks(
	ctx context.Context,
	reqCh chan taskDetail,
) error {

	defer close(reqCh)

	iter := collection.NewPagingIteratorWithToken(s.getPaginationFn(ctx), s.hbd.NextPageToken)
	for iter.HasNext() {
		if err := s.rateLimiter.Wait(ctx); err != nil {
			// context done
			return err
		}

		item, err := iter.Next()
		if err != nil {
			return err
		}

		// Heartbeat to prevent heartbeat timeout.
		s.heartbeat(ctx)

		task := s.filterTask(item)
		if task == nil {
			continue
		}

		select {
		case reqCh <- *task:
			// noop

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *Scavenger) taskWorker(
	ctx context.Context,
	taskCh chan taskDetail,
) {

	defer s.WaitGroup.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-taskCh:
			if !ok {
				return
			}

			s.heartbeat(ctx)
			s.handleErr(s.handleTask(ctx, task))
		}
	}
}

func (s *Scavenger) heartbeat(ctx context.Context) {
	s.Lock()
	defer s.Unlock()

	if !s.isInTest {
		activity.RecordHeartbeat(ctx, s.hbd)
	}
}

func (s *Scavenger) filterTask(
	branch persistence.HistoryBranchDetail,
) *taskDetail {

	if time.Now().UTC().Add(-s.historyDataMinAge()).Before(timestamp.TimeValue(branch.ForkTime)) {
		metrics.HistoryScavengerSkipCount.With(s.metricsHandler).Record(1)

		s.Lock()
		defer s.Unlock()
		s.hbd.SkipCount++
		return nil
	}

	namespaceID, workflowID, runID, err := persistence.SplitHistoryGarbageCleanupInfo(branch.Info)
	if err != nil {
		s.logger.Error("unable to parse the history cleanup info", tag.DetailInfo(branch.Info), tag.Error(err))
		metrics.HistoryScavengerErrorCount.With(s.metricsHandler).Record(1)

		s.Lock()
		defer s.Unlock()
		s.hbd.ErrorCount++
		return nil
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID, workflowID, s.numShards)

	branchToken, err := serialization.HistoryBranchToBlob(branch.BranchInfo)
	if err != nil {
		s.logger.Error("unable to serialize the history branch token", tag.DetailInfo(branch.Info), tag.Error(err))
		metrics.HistoryScavengerErrorCount.With(s.metricsHandler).Record(1)

		s.Lock()
		defer s.Unlock()
		s.hbd.ErrorCount++
		return nil
	}

	return &taskDetail{
		shardID:     shardID,
		namespaceID: namespaceID,
		workflowID:  workflowID,
		runID:       runID,
		branchToken: branchToken.Data,
	}
}

func (s *Scavenger) handleTask(
	ctx context.Context,
	task taskDetail,
) error {
	// this checks if the mutableState still exists
	// if not then the history branch is garbage, we need to delete the history branch
	ms, err := s.client.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: task.namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.workflowID,
			RunId:      task.runID,
		},
	})
	switch err.(type) {
	case nil:
		if s.enableRetentionVerification() {
			return s.cleanUpWorkflowPastRetention(ctx, ms.GetDatabaseMutableState())
		}
		return nil
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		// case handled below
	default:
		s.logger.Error("encounter error when describing the mutable state", getTaskLoggingTags(err, task)...)
		return err
	}

	//deleting history branch
	err = s.db.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
		ShardID:     task.shardID,
		BranchToken: task.branchToken,
	})
	if err != nil {
		s.logger.Error("encountered error when deleting garbage history branch", getTaskLoggingTags(err, task)...)
	} else {
		s.logger.Info("deleted history garbage", getTaskLoggingTags(nil, task)...)
	}
	return err
}

func (s *Scavenger) handleErr(
	err error,
) {
	s.Lock()
	defer s.Unlock()
	if err != nil {
		metrics.HistoryScavengerErrorCount.With(s.metricsHandler).Record(1)
		s.hbd.ErrorCount++
		return
	}

	metrics.HistoryScavengerSuccessCount.With(s.metricsHandler).Record(1)
	s.hbd.SuccessCount++
}

func (s *Scavenger) getPaginationFn(
	ctx context.Context,
) collection.PaginationFn[persistence.HistoryBranchDetail] {
	return func(paginationToken []byte) ([]persistence.HistoryBranchDetail, []byte, error) {
		req := &persistence.GetAllHistoryTreeBranchesRequest{
			PageSize:      pageSize,
			NextPageToken: paginationToken,
		}
		resp, err := s.db.GetAllHistoryTreeBranches(ctx, req)
		if err != nil {
			return nil, nil, err
		}
		paginateItems := resp.Branches

		s.Lock()
		s.hbd.CurrentPage++
		s.hbd.NextPageToken = resp.NextPageToken
		s.Unlock()

		return paginateItems, resp.NextPageToken, nil
	}
}

func (s *Scavenger) cleanUpWorkflowPastRetention(
	ctx context.Context,
	mutableState *persistencepb.WorkflowMutableState,
) error {
	if mutableState.GetExecutionState().GetState() != enums.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// Skip running workflow
		return nil
	}

	executionInfo := mutableState.GetExecutionInfo()
	ns, err := s.registry.GetNamespaceByID(namespace.ID(executionInfo.GetNamespaceId()))
	switch err.(type) {
	case *serviceerror.NamespaceNotFound:
		// TODO delete the workflow data after issue #3536 close
		return nil
	case nil:
		// continue to delete
	default:
		return err
	}

	retention := ns.Retention()
	finalUpdateTime := executionInfo.GetLastUpdateTime()
	age := time.Now().UTC().Sub(timestamp.TimeValue(finalUpdateTime))
	if age > retention+s.executionDataDurationBuffer() {
		_, err = s.adminClient.DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
			Namespace: ns.Name().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.GetWorkflowId(),
				RunId:      mutableState.GetExecutionState().GetRunId(),
			},
		})
		if err != nil {
			// This is experimental. Ignoring the error so it will not block the history scavenger.
			s.logger.Warn("Failed to delete workflow past retention in history scavenger",
				tag.Error(err),
				tag.WorkflowNamespace(ns.Name().String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
			)
			return nil
		}
		s.logger.Info("Delete workflow data past retention via history scavenger",
			tag.WorkflowNamespace(ns.Name().String()),
			tag.WorkflowID(executionInfo.GetWorkflowId()),
			tag.WorkflowRunID(mutableState.GetExecutionState().GetRunId()),
		)
	}
	return nil
}

func getTaskLoggingTags(err error, task taskDetail) []tag.Tag {
	if err != nil {
		return []tag.Tag{
			tag.Error(err),
			tag.WorkflowNamespaceID(task.namespaceID),
			tag.WorkflowID(task.workflowID),
			tag.WorkflowRunID(task.runID),
			tag.WorkflowBranchToken(task.branchToken),
		}
	}
	return []tag.Tag{
		tag.WorkflowNamespaceID(task.namespaceID),
		tag.WorkflowID(task.workflowID),
		tag.WorkflowRunID(task.runID),
		tag.WorkflowBranchToken(task.branchToken),
	}
}
