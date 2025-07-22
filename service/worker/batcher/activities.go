package batcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/pborman/uuid"
	activitypb "go.temporal.io/api/activity/v1"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	pageSize                 = 1000
	statusRunningQueryFilter = "ExecutionStatus='Running'"
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

// BatchActivity is an activity for processing batch operation.
func (a *activities) BatchActivity(ctx context.Context, batchParams *batchpb.BatchOperation) (HeartBeatDetails, error) {
	logger := a.getActivityLogger(ctx)
	hbd := HeartBeatDetails{}
	metricsHandler := a.MetricsHandler.WithTags(metrics.OperationTag(metrics.BatcherScope), metrics.NamespaceTag(batchParams.Namespace))

	if err := a.checkNamespace(batchParams.Namespace); err != nil {
		metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
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

	adjustedQuery := a.adjustQuery(batchParams)

	if startOver {
		estimateCount := int64(len(batchParams.WorkflowExecutions))
		if len(adjustedQuery) > 0 {
			resp, err := sdkClient.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
				Query: adjustedQuery,
			})
			if err != nil {
				metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
				logger.Error("Failed to get estimate workflow count", tag.Error(err))
				return HeartBeatDetails{}, err
			}
			estimateCount = resp.GetCount()
		}
		hbd.TotalEstimate = estimateCount
	}
	rps := a.getOperationRPS(batchParams.Rps)
	rateLimit := rate.Limit(rps)
	burstLimit := int(math.Ceil(rps)) // should never be zero because everything would be rejected
	rateLimiter := rate.NewLimiter(rateLimit, burstLimit)
	taskCh := make(chan taskDetail, pageSize)
	respCh := make(chan error, pageSize)
	for i := 0; i < a.getOperationConcurrency(int(batchParams.Concurrency)); i++ {
		go startTaskProcessor(ctx, batchParams, taskCh, respCh, rateLimiter, sdkClient, a.FrontendClient, metricsHandler, logger)
	}

	for {
		executions := batchParams.WorkflowExecutions
		pageToken := hbd.PageToken
		if len(adjustedQuery) > 0 {
			resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				PageSize:      int32(pageSize),
				NextPageToken: pageToken,
				Query:         adjustedQuery,
			})
			if err != nil {
				metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
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
				execution: wf,
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
				metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
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

func (a *activities) adjustQuery(batchParams *batchpb.BatchOperation) string {
	if len(batchParams.Query) == 0 {
		// don't add anything if query is empty
		return batchParams.Query
	}

	switch batchParams.GetOperation().(type) {
	case *batchpb.BatchOperation_TerminationOperation, *batchpb.BatchOperation_CancellationOperation, *batchpb.BatchOperation_SignalOperation, *batchpb.BatchOperation_UpdateWorkflowExecutionOptionsOperation, *batchpb.BatchOperation_UnpauseActivitiesOperation:
		return fmt.Sprintf("(%s) AND (%s)", batchParams.Query, statusRunningQueryFilter)
	default:
		return batchParams.Query
	}
}

func (a *activities) getOperationRPS(requestedRPS float64) float64 {
	maxRPS := float64(a.rps(a.namespace.String()))
	if requestedRPS <= 0 || requestedRPS > maxRPS {
		return maxRPS
	}
	return requestedRPS
}

func (a *activities) getOperationConcurrency(concurrency int) int {
	if concurrency <= 0 {
		return a.concurrency(a.namespace.String())
	}
	return concurrency
}

func startTaskProcessor(
	ctx context.Context,
	batchParams *batchpb.BatchOperation,
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

			switch operation := batchParams.Operation.(type) {
			case *batchpb.BatchOperation_TerminationOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						return sdkClient.TerminateWorkflow(ctx, workflowID, runID, batchParams.Reason)
					})
			case *batchpb.BatchOperation_CancellationOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						return sdkClient.CancelWorkflow(ctx, workflowID, runID)
					})
			case *batchpb.BatchOperation_SignalOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						_, err := frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
							Namespace: batchParams.Namespace,
							WorkflowExecution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							SignalName: operation.SignalOperation.GetSignal(),
							Input:      operation.SignalOperation.GetInput(),
						})
						return err
					})
			case *batchpb.BatchOperation_DeletionOperation:
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
			case *batchpb.BatchOperation_ResetOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						workflowExecution := &commonpb.WorkflowExecution{
							WorkflowId: workflowID,
							RunId:      runID,
						}
						var eventId int64
						var err error
						var resetReapplyType enumspb.ResetReapplyType
						var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
						if operation.ResetOperation.Options != nil {
							// Using ResetOptions
							// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByOptions(ctx, operation.ResetOperation.Options, batchParams.Namespace, workflowExecution, frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = operation.ResetOperation.Options.ResetReapplyType
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyExcludeTypes = operation.ResetOperation.Options.ResetReapplyExcludeTypes
						} else {
							// Old fields
							eventId, err = getResetEventIDByType(ctx, operation.ResetOperation.ResetType, batchParams.Namespace, workflowExecution, frontendClient, logger)
							resetReapplyType = operation.ResetOperation.ResetReapplyType
						}
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
							Namespace:                 batchParams.Namespace,
							WorkflowExecution:         workflowExecution,
							Reason:                    batchParams.Reason,
							RequestId:                 uuid.New(),
							WorkflowTaskFinishEventId: eventId,
							ResetReapplyType:          resetReapplyType,
							ResetReapplyExcludeTypes:  resetReapplyExcludeTypes,
							PostResetOperations:       operation.ResetOperation.PostResetOperations,
						})
						return err
					})
			case *batchpb.BatchOperation_UnpauseActivitiesOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						unpauseRequest := &workflowservice.UnpauseActivityRequest{
							Namespace: batchParams.Namespace,
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							Identity:       operation.UnpauseActivitiesOperation.Identity,
							ResetAttempts:  !operation.UnpauseActivitiesOperation.ResetAttempts,
							ResetHeartbeat: operation.UnpauseActivitiesOperation.ResetHeartbeat,
							Jitter:         operation.UnpauseActivitiesOperation.Jitter,
						}

						switch ao := operation.UnpauseActivitiesOperation.GetActivity().(type) {
						case *batchpb.BatchOperationUnpauseActivities_Type:
							unpauseRequest.Activity = &workflowservice.UnpauseActivityRequest_Type{
								Type: ao.Type,
							}
						case *batchpb.BatchOperationUnpauseActivities_MatchAll:
							unpauseRequest.Activity = &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true}
						}

						_, err = frontendClient.UnpauseActivity(ctx, unpauseRequest)
						return err
					})

			case *batchpb.BatchOperation_UpdateWorkflowExecutionOptionsOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						var err error
						_, err = frontendClient.UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
							Namespace: batchParams.Namespace,
							WorkflowExecution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							WorkflowExecutionOptions: operation.UpdateWorkflowExecutionOptionsOperation.WorkflowExecutionOptions,
							UpdateMask:               &fieldmaskpb.FieldMask{Paths: operation.UpdateWorkflowExecutionOptionsOperation.UpdateMask.Paths},
						})
						return err
					})

			case *batchpb.BatchOperation_ResetActivitiesOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						resetRequest := &workflowservice.ResetActivityRequest{
							Namespace: batchParams.Namespace,
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							Identity:               operation.ResetActivitiesOperation.Identity,
							ResetHeartbeat:         operation.ResetActivitiesOperation.ResetHeartbeat,
							Jitter:                 operation.ResetActivitiesOperation.Jitter,
							KeepPaused:             operation.ResetActivitiesOperation.KeepPaused,
							RestoreOriginalOptions: operation.ResetActivitiesOperation.RestoreOriginalOptions,
						}

						switch ao := operation.ResetActivitiesOperation.GetActivity().(type) {
						case *batchpb.BatchOperationResetActivities_Type:
							resetRequest.Activity = &workflowservice.ResetActivityRequest_Type{Type: ao.Type}
						case *batchpb.BatchOperationResetActivities_MatchAll:
							resetRequest.Activity = &workflowservice.ResetActivityRequest_MatchAll{MatchAll: true}
						}

						_, err = frontendClient.ResetActivity(ctx, resetRequest)
						return err
					})
			case *batchpb.BatchOperation_UpdateActivityOptionsOperation:
				err = processTask(ctx, limiter, task,
					func(workflowID, runID string) error {
						updateRequest := &workflowservice.UpdateActivityOptionsRequest{
							Namespace: batchParams.Namespace,
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: workflowID,
								RunId:      runID,
							},
							UpdateMask:      &fieldmaskpb.FieldMask{Paths: operation.UpdateActivityOptionsOperation.UpdateMask.Paths},
							RestoreOriginal: operation.UpdateActivityOptionsOperation.RestoreOriginal,
							Identity:        operation.UpdateActivityOptionsOperation.Identity,
						}

						switch ao := operation.UpdateActivityOptionsOperation.GetActivity().(type) {
						case *batchpb.BatchOperationUpdateActivityOptions_Type:
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Type{Type: ao.Type}
						case *batchpb.BatchOperationUpdateActivityOptions_MatchAll:
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_MatchAll{MatchAll: true}
						}

						if ao := operation.UpdateActivityOptionsOperation.GetActivityOptions(); ao != nil {
							updateRequest.ActivityOptions = &activitypb.ActivityOptions{
								ScheduleToStartTimeout: ao.ScheduleToStartTimeout,
								ScheduleToCloseTimeout: ao.ScheduleToCloseTimeout,
								StartToCloseTimeout:    ao.StartToCloseTimeout,
								HeartbeatTimeout:       ao.HeartbeatTimeout,
							}

							if rp := ao.RetryPolicy; rp != nil {
								updateRequest.ActivityOptions.RetryPolicy = &commonpb.RetryPolicy{
									InitialInterval:        rp.InitialInterval,
									BackoffCoefficient:     rp.BackoffCoefficient,
									MaximumInterval:        rp.MaximumInterval,
									MaximumAttempts:        rp.MaximumAttempts,
									NonRetryableErrorTypes: rp.NonRetryableErrorTypes,
								}
							}
						}
						_, err = frontendClient.UpdateActivityOptions(ctx, updateRequest)
						return err
					})
				// QUESTION seankane (2025-07-18): why do we not have a default case and return an error? @yuri/@chetan
			}
			if err != nil {
				metrics.BatcherProcessorFailures.With(metricsHandler).Record(1)
				logger.Error("Failed to process batch operation task", tag.Error(err))

				nonRetryable := false
				for _, errType := range batchParams.NonRetryableErrors {
					if errType == err.Error() {
						nonRetryable = true
						break
					}
				}
				if nonRetryable || task.attempts > int(batchParams.AttemptsOnRetryableError) {
					respCh <- err
				} else {
					// put back to the channel if less than attemptsOnError
					task.attempts++
					taskCh <- task
				}
			} else {
				metrics.BatcherProcessorSuccess.With(metricsHandler).Record(1)
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
		if !common.IsNotFoundError(err) {
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

func getResetEventIDByType(
	ctx context.Context,
	resetType enumspb.ResetType,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger,
) (int64, error) {
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

// Note: may modify workflowExecution.RunId
func getResetEventIDByOptions(
	ctx context.Context,
	resetOptions *commonpb.ResetOptions,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger,
) (int64, error) {
	switch target := resetOptions.Target.(type) {
	case *commonpb.ResetOptions_FirstWorkflowTask:
		return getFirstWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	case *commonpb.ResetOptions_LastWorkflowTask:
		return getLastWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	case *commonpb.ResetOptions_WorkflowTaskId:
		return target.WorkflowTaskId, nil
	case *commonpb.ResetOptions_BuildId:
		return getResetPoint(ctx, namespaceStr, workflowExecution, frontendClient, target.BuildId, resetOptions.CurrentRunOnly)
	default:
		errorMsg := fmt.Sprintf("provided reset target (%+v) is not supported.", resetOptions.Target)
		return 0, serviceerror.NewInvalidArgument(errorMsg)
	}
}

func getLastWorkflowTaskEventID(
	ctx context.Context,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger,
) (workflowTaskEventID int64, err error) {
	req := &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistoryReverse(ctx, req)
		if err != nil {
			logger.Error("failed to run GetWorkflowExecutionHistoryReverse", tag.Error(err))
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

func getFirstWorkflowTaskEventID(
	ctx context.Context,
	namespaceStr string,
	workflowExecution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger,
) (workflowTaskEventID int64, err error) {
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}
	for {
		resp, err := frontendClient.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			logger.Error("failed to run GetWorkflowExecutionHistory", tag.Error(err))
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

func getResetPoint(
	ctx context.Context,
	namespaceStr string,
	execution *commonpb.WorkflowExecution,
	frontendClient workflowservice.WorkflowServiceClient,
	buildId string,
	currentRunOnly bool,
) (workflowTaskEventID int64, err error) {
	res, err := frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespaceStr,
		Execution: execution,
	})
	if err != nil {
		return 0, err
	}
	resetPoints := res.GetWorkflowExecutionInfo().GetAutoResetPoints().GetPoints()
	for _, point := range resetPoints {
		if point.BuildId == buildId {
			if !point.Resettable {
				return 0, fmt.Errorf("Reset point for %v is not resettable", buildId)
			} else if point.ExpireTime != nil && point.ExpireTime.AsTime().Before(time.Now()) {
				return 0, fmt.Errorf("Reset point for %v is expired", buildId)
			} else if execution.RunId != point.RunId && currentRunOnly {
				return 0, fmt.Errorf("Reset point for %v points to previous run and CurrentRunOnly is set", buildId)
			}
			execution.RunId = point.RunId
			return point.FirstWorkflowTaskCompletedId, nil
		}
	}
	return 0, fmt.Errorf("Can't find reset point for %v", buildId)
}
