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

package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	VerifyStatus int
	VerifyResult struct {
		Status VerifyStatus
		Reason string
	}

	replicationTasksHeartbeatDetails struct {
		Results                       []VerifyResult
		CheckPoint                    time.Time
		LastNotFoundWorkflowExecution commonpb.WorkflowExecution
	}

	verifyReplicationTasksTimeoutErr struct {
		timeout time.Duration
		details replicationTasksHeartbeatDetails
	}
)

// State Diagram
//
//		     NOT_VERIFIED
//		         │
//		┌────────┴─────────┐
//		│                  │
//	 VERIFIED      VERIFIED_SKIPPED
const (
	NOT_VERIFIED   VerifyStatus = 0
	VERIFIED       VerifyStatus = 1
	VERIFY_SKIPPED VerifyStatus = 2
)

func (r VerifyResult) isNotVerified() bool {
	return r.Status == NOT_VERIFIED
}

func (r VerifyResult) isVerified() bool {
	return r.Status == VERIFIED
}

func (r VerifyResult) isSkipped() bool {
	return r.Status == VERIFY_SKIPPED
}

func (r VerifyResult) isCompleted() bool {
	return r.isVerified() || r.isSkipped()
}

func (e verifyReplicationTasksTimeoutErr) Error() string {
	return fmt.Sprintf("verifyReplicationTasks was not able to make progress for more than %v minutes (retryable). Not found WorkflowExecution: %v,",
		e.timeout,
		e.details.LastNotFoundWorkflowExecution,
	)
}

func (a *activities) verifyHandleNotFoundWorkflow(
	ctx context.Context,
	namespaceID string,
	we *commonpb.WorkflowExecution,
	result *VerifyResult,
) error {
	tags := []tag.Tag{tag.WorkflowType(forceReplicationWorkflowName), tag.WorkflowNamespaceID(namespaceID), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId)}
	resp, err := a.historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   we,
	})

	if err != nil {
		if isNotFoundServiceError(err) {
			// Workflow could be deleted due to retention.
			result.Status = VERIFY_SKIPPED
			result.Reason = reasonWorkflowNotFound
			return nil
		}

		return err
	}

	if resp.GetDatabaseMutableState().GetExecutionState().GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		a.forceReplicationMetricsHandler.Counter(metrics.EncounterZombieWorkflowCount.GetMetricName()).Record(1)
		a.logger.Info("createReplicationTasks skip Zombie workflow", tags...)
		result.Status = VERIFY_SKIPPED
		result.Reason = reasonZombieWorkflow
	}

	return nil
}

func (a *activities) verifyReplicationTasksV1(
	ctx context.Context,
	request *verifyReplicationTasksRequest,
	detail *replicationTasksHeartbeatDetails,
	remoteClient adminservice.AdminServiceClient,
) (verified bool, progress bool, err error) {
	start := time.Now()
	defer func() {
		a.forceReplicationMetricsHandler.Timer(metrics.VerifyReplicationTasksLatency.GetMetricName()).Record(time.Since(start))
	}()

	progress = false
	for i := 0; i < len(request.Executions); i++ {
		r := &detail.Results[i]
		we := request.Executions[i]
		if r.isCompleted() {
			continue
		}

		s := time.Now()
		// Check if execution exists on remote cluster
		_, err := remoteClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: request.Namespace,
			Execution: &we,
		})
		a.forceReplicationMetricsHandler.Timer(metrics.VerifyDescribeMutableStateLatency.GetMetricName()).Record(time.Since(s))

		switch err.(type) {
		case nil:
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskSuccess.GetMetricName()).Record(1)
			r.Status = VERIFIED
			progress = true

		case *serviceerror.NotFound:
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace)).Counter(metrics.VerifyReplicationTaskNotFound.GetMetricName()).Record(1)
			if err := a.verifyHandleNotFoundWorkflow(ctx, request.NamespaceID, &we, r); err != nil {
				return false, progress, err
			}

			if r.isNotVerified() {
				detail.LastNotFoundWorkflowExecution = we
				return false, progress, nil
			}

			progress = true

		default:
			a.forceReplicationMetricsHandler.WithTags(metrics.NamespaceTag(request.Namespace), metrics.ServiceErrorTypeTag(err)).
				Counter(metrics.VerifyReplicationTaskFailed.GetMetricName()).Record(1)

			return false, progress, errors.WithMessage(err, "remoteClient.DescribeMutableState call failed")
		}
	}

	return true, progress, nil
}

func (a *activities) VerifyReplicationTasks(ctx context.Context, request *verifyReplicationTasksRequest) error {
	ctx = headers.SetCallerInfo(ctx, headers.NewPreemptableCallerInfo(request.Namespace))
	remoteClient := a.clientFactory.NewRemoteAdminClientWithTimeout(
		request.TargetClusterEndpoint,
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	var details replicationTasksHeartbeatDetails
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &details); err != nil {
			return err
		}
	} else {
		details.Results = make([]VerifyResult, len(request.Executions))
		details.CheckPoint = time.Now()
		activity.RecordHeartbeat(ctx, details)
	}

	// Verify if replication tasks exist on target cluster. There are several cases where execution was not found on target cluster.
	//  1. replication lag
	//  2. Zombie workflow execution
	//  3. workflow execution was deleted (due to retention) after replication task was created
	//  4. workflow execution was not applied succesfully on target cluster (i.e, bug)
	//
	// The verification step is retried for every VerifyInterval to handle #1. Verification progress
	// is recorded in activity heartbeat. The verification is considered of making progress if there was at least one new execution
	// being verified. If no progress is made for long enough, then
	//  - more than RetryableTimeout, the activity fails with retryable error and activity will restart. This gives us the chance
	//    to identify case #2 and #3 by rerunning createReplicationTasks.
	//  - more than NonRetryableTimeout, it means potentially we encountered #4. The activity returns
	//    non-retryable error and force-replication workflow will restarted.
	for {

		// Since replication has a lag, sleep first.
		time.Sleep(request.VerifyInterval)

		verified, progress, err := a.verifyReplicationTasksV1(ctx, request, &details, remoteClient)
		if err != nil {
			return err
		}

		if progress {
			details.CheckPoint = time.Now()
		}

		activity.RecordHeartbeat(ctx, details)

		if verified == true {
			return nil
		}

		diff := time.Now().Sub(details.CheckPoint)
		if diff > defaultNoProgressNotRetryableTimeout {
			// Potentially encountered a missing execution, return non-retryable error
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("verifyReplicationTasks was not able to make progress for more than %v minutes (not retryable). Not found WorkflowExecution: %v, Checkpoint: %v",
					diff.Minutes(),
					details.LastNotFoundWorkflowExecution, details.CheckPoint),
				"", nil)
		}
	}
}
