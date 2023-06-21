// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package batcher

import (
	"context"
	"errors"
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"golang.org/x/time/rate"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
)

var (
	errNamespaceMismatch = errors.New("namespace mismatch")
)

type activities struct {
	activityDeps
	namespace   namespace.Name
	namespaceID namespace.ID
	rps         dynamicconfig.IntPropertyFnWithNamespaceFilter
	concurrency dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (a *activities) checkNamespace(namespace string) error {
	// Ignore system namespace for backward compatibility.
	// TODO: Remove the system namespace special handling after 1.19+
	if namespace != a.namespace.String() && a.namespace.String() != primitives.SystemLocalNamespace {
		return errNamespaceMismatch
	}
	return nil
}

// BatchActivity is activity for processing batch operation
func (a *activities) BatchActivity(ctx context.Context, batchParams BatchParams) (HeartBeatDetails, error) {
	logger := a.getActivityLogger(ctx)
	hbd := HeartBeatDetails{}
	metricsHandler := a.MetricsHandler.WithTags(metrics.OperationTag(metrics.BatcherScope), metrics.NamespaceTag(batchParams.Namespace))

	if err := a.checkNamespace(batchParams.Namespace); err != nil {
		metricsHandler.Counter(metrics.BatcherOperationFailures.GetMetricName()).Record(1)
		logger.Error("Failed to run batch operation due to namespace mismatch", tag.Error(err))
		return hbd, err
	}

	sdkClient := a.ClientFactory.NewClient(sdkclient.Options{
		Namespace:     batchParams.Namespace,
		DataConverter: sdk.PreferProtoDataConverter,
	})
	startOver := true
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &hbd); err == nil {
			startOver = false
		} else {
			logger.Error("Failed to recover from last heartbeat, start over from beginning", tag.Error(err))
		}
	}

	if startOver {
		estimateCount := int64(len(batchParams.Executions))
		if len(batchParams.Query) > 0 {
			resp, err := sdkClient.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
				Query: batchParams.Query,
			})
			if err != nil {
				metricsHandler.Counter(metrics.BatcherOperationFailures.GetMetricName()).Record(1)
				logger.Error("Failed to get estimate workflow count", tag.Error(err))
				return HeartBeatDetails{}, err
			}
			estimateCount = resp.GetCount()
		}
		hbd.TotalEstimate = estimateCount
	}
	rps := a.getOperationRPS(batchParams.RPS)
	rateLimiter := rate.NewLimiter(rate.Limit(rps), rps)
	taskCh := make(chan taskDetail, pageSize)
	respCh := make(chan error, pageSize)
	for i := 0; i < a.getOperationConcurrency(batchParams.Concurrency); i++ {
		go startTaskProcessor(ctx, batchParams, taskCh, respCh, rateLimiter, sdkClient, a.FrontendClient, metricsHandler, logger)
	}

	for {
		executions := batchParams.Executions
		pageToken := hbd.PageToken
		if len(batchParams.Query) > 0 {
			resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				PageSize:      int32(pageSize),
				NextPageToken: pageToken,
				Query:         batchParams.Query,
			})
			if err != nil {
				metricsHandler.Counter(metrics.BatcherOperationFailures.GetMetricName()).Record(1)
				logger.Error("Failed to list workflow executions", tag.Error(err))
				return HeartBeatDetails{}, err
			}
			pageToken = resp.NextPageToken
			for _, wf := range resp.Executions {
				executions = append(executions, wf.Execution)
			}
		}

		batchCount := len(executions)
		if batchCount <= 0 {
			break
		}
		// send all tasks
		for _, wf := range executions {
			taskCh <- taskDetail{
				execution: *wf,
				attempts:  1,
				hbd:       hbd,
			}
		}

		succCount := 0
		errCount := 0
		// wait for counters indicate this batch is done
	Loop:
		for {
			select {
			case err := <-respCh:
				if err == nil {
					succCount++
				} else {
					errCount++
				}
				if succCount+errCount == batchCount {
					break Loop
				}
			case <-ctx.Done():
				metricsHandler.Counter(metrics.BatcherOperationFailures.GetMetricName()).Record(1)
				logger.Error("Failed to complete batch operation", tag.Error(ctx.Err()))
				return HeartBeatDetails{}, ctx.Err()
			}
		}

		hbd.CurrentPage++
		hbd.PageToken = pageToken
		hbd.SuccessCount += succCount
		hbd.ErrorCount += errCount
		activity.RecordHeartbeat(ctx, hbd)

		if len(hbd.PageToken) == 0 {
			break
		}
	}

	return hbd, nil
}

func (a *activities) getActivityLogger(ctx context.Context) log.Logger {
	wfInfo := activity.GetInfo(ctx)
	return log.With(
		a.Logger,
		tag.WorkflowID(wfInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(wfInfo.WorkflowExecution.RunID),
		tag.WorkflowNamespace(wfInfo.WorkflowNamespace),
	)
}

func (a *activities) getOperationRPS(rps int) int {
	if rps <= 0 {
		return a.rps(a.namespace.String())
	}
	return rps
}

func (a *activities) getOperationConcurrency(concurrency int) int {
	if concurrency <= 0 {
		return a.concurrency(a.namespace.String())
	}
	return concurrency
}

func startTaskProcessor(
	ctx context.Context,
	batchParams BatchParams,
	taskCh chan taskDetail,
	respCh chan error,
	limiter *rate.Limiter,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-taskCh:
			if isDone(ctx) {
				return
			}
			var err error

			switch batchParams.BatchType {
			case BatchTypeTerminate:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						return sdkClient.TerminateWorkflow(ctx, workflowID, runID, batchParams.Reason)
					})
			case BatchTypeCancel:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						return sdkClient.CancelWorkflow(ctx, workflowID, runID)
					})
			case BatchTypeSignal:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						_, err := frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
							Namespace: batchParams.Namespace,
							WorkflowExecution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							SignalName: batchParams.SignalParams.SignalName,
							Input:      batchParams.SignalParams.Input,
						})
						return err
					})
			case BatchTypeDelete:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						_, err := frontendClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
							Namespace: batchParams.Namespace,
							WorkflowExecution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
						})
						return err
					})
			case BatchTypeReset:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						workflowExecution := &commonpb.WorkflowExecution{
							WorkflowId: workflowID,
							RunId:      runID,
						}
						eventId, err := getResetEventIDByType(ctx, batchParams.ResetParams.ResetType, batchParams.Namespace, workflowExecution, frontendClient, logger)
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
							Namespace:                 batchParams.Namespace,
							WorkflowExecution:         workflowExecution,
							Reason:                    batchParams.Reason,
							WorkflowTaskFinishEventId: eventId,
							RequestId:                 uuid.New(),
							ResetReapplyType:          batchParams.ResetParams.ResetReapplyType,
						})
						return err
					})
			}
			if err != nil {
				metricsHandler.Counter(metrics.BatcherProcessorFailures.GetMetricName()).Record(1)
				logger.Error("Failed to process batch operation task", tag.Error(err))

				_, ok := batchParams._nonRetryableErrors[err.Error()]
				if ok || task.attempts > batchParams.AttemptsOnRetryableError {
					respCh <- err
				} else {
					// put back to the channel if less than attemptsOnError
					task.attempts++
					taskCh <- task
				}
			} else {
				metricsHandler.Counter(metrics.BatcherProcessorSuccess.GetMetricName()).Record(1)
				respCh <- nil
			}
		}
	}
}

func processTask(
	ctx context.Context,
	limiter *rate.Limiter,
	task taskDetail,
	procFn func(string, string) error,
) error {

	err := limiter.Wait(ctx)
	if err != nil {
		return err
	}
	activity.RecordHeartbeat(ctx, task.hbd)

	err = procFn(task.execution.GetWorkflowId(), task.execution.GetRunId())
	if err != nil {
		// NotFound means wf is not running or deleted
		if _, isNotFound := err.(*serviceerror.NotFound); !isNotFound {
			return err
		}
	}

	return nil
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func getResetEventIDByType(ctx context.Context,
	resetType enumspb.ResetType,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger) (int64, error) {
	switch resetType {
	case enumspb.RESET_TYPE_FIRST_WORKFLOW_TASK:
		return getFirstWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	case enumspb.RESET_TYPE_LAST_WORKFLOW_TASK:
		return getLastWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	default:
		errorMsg := fmt.Sprintf("provided reset type (%v) is not supported.", resetType)
		return 0, serviceerror.NewInvalidArgument(errorMsg)
	}
}

func getLastWorkflowTaskEventID(ctx context.Context,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger) (workflowTaskEventID int64, err error) {
	req := &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistoryReverse(ctx, req)
		if err != nil {
			logger.Error("failed to run GetWorkflowExecutionHistoryReverse")
			return 0, errors.New("failed to get workflow execution history")
		}
		for _, e := range resp.GetHistory().GetEvents() {
			switch e.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
				workflowTaskEventID = e.GetEventId()
				return workflowTaskEventID, nil
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
				// if there is no task completed event, set it to first scheduled event + 1
				workflowTaskEventID = e.GetEventId() + 1
			}
		}
		if len(resp.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = resp.NextPageToken
	}
	if workflowTaskEventID == 0 {
		return 0, errors.New("unable to find any scheduled or completed task")
	}
	return
}

func getFirstWorkflowTaskEventID(ctx context.Context,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger) (workflowTaskEventID int64, err error) {
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			logger.Error("failed to run GetWorkflowExecutionHistory")
			return 0, errors.New("GetWorkflowExecutionHistory failed")
		}
		for _, e := range resp.GetHistory().GetEvents() {
			switch e.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
				workflowTaskEventID = e.GetEventId()
				return workflowTaskEventID, nil
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
				if workflowTaskEventID == 0 {
					workflowTaskEventID = e.GetEventId() + 1
				}
			}
		}
		if len(resp.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = resp.NextPageToken
	}
	if workflowTaskEventID == 0 {
		return 0, errors.New("unable to find any scheduled or completed task")
	}
	return
}
