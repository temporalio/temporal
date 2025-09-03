package batcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/pborman/uuid"
	activitypb "go.temporal.io/api/activity/v1"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	pageSize                 = 1000
	statusRunningQueryFilter = "ExecutionStatus='Running'"
)

var (
	errNamespaceMismatch = errors.New("namespace mismatch")
)

// batchProcessorConfig holds the configuration for batch processing
type batchProcessorConfig struct {
	namespace         string
	adjustedQuery     string
	rps               float64
	concurrency       int
	initialPageToken  []byte
	initialExecutions []*commonpb.WorkflowExecution
}

// batchWorkerProcessor defines the interface for different worker processor types
type batchWorkerProcessor func(
	ctx context.Context,
	taskCh chan task,
	respCh chan taskResponse,
	rateLimiter *rate.Limiter,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
)

// page represents a page of workflow executions to be processed
type page struct {
	executions     []*commonpb.WorkflowExecution
	submittedCount int
	successCount   int
	errorCount     int
	nextPageToken  []byte
	pageNumber     int
	prev, next     *page
}

// hasNext returns true if there are more pages to fetch
func (p *page) hasNext() bool {
	return len(p.nextPageToken) > 0
}

// allSubmitted returns true if all executions in this page have been submitted
func (p *page) allSubmitted() bool {
	return p.submittedCount == len(p.executions)
}

// nextTask returns the next task to be submitted from this page
func (p *page) nextTask() task {
	if p.submittedCount >= len(p.executions) {
		return task{} // No more tasks in this page
	}

	task := task{
		execution: p.executions[p.submittedCount],
		attempts:  1,
		page:      p,
	}
	return task
}

// done returns true if this page and all previous pages are complete
func (p *page) done() bool {
	if p.prev != nil && !p.prev.done() {
		return false
	}
	return p.successCount+p.errorCount == len(p.executions)
}

// fetchPage fetches a new page of workflow executions
func fetchPage(
	ctx context.Context,
	sdkClient sdkclient.Client,
	config batchProcessorConfig,
	pageToken []byte,
	pageNumber int,
) (*page, error) {
	if len(config.adjustedQuery) == 0 {
		// No query provided, return empty page
		return &page{
			executions:    []*commonpb.WorkflowExecution{},
			nextPageToken: []byte{},
			pageNumber:    pageNumber,
		}, nil
	}

	resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		PageSize:      int32(pageSize),
		NextPageToken: pageToken,
		Query:         config.adjustedQuery,
	})
	if err != nil {
		return nil, err
	}

	executions := make([]*commonpb.WorkflowExecution, 0, len(resp.Executions))
	for _, wf := range resp.Executions {
		executions = append(executions, wf.Execution)
	}

	return &page{
		executions:    executions,
		nextPageToken: resp.NextPageToken,
		pageNumber:    pageNumber,
	}, nil
}

// processWorkflowsWithProactiveFetching handles the core logic for both batch activity functions
// nolint:revive,cognitive-complexity
func (a *activities) processWorkflowsWithProactiveFetching(
	ctx context.Context,
	config batchProcessorConfig,
	startWorkerProcessor batchWorkerProcessor,
	sdkClient sdkclient.Client,
	metricsHandler metrics.Handler,
	logger log.Logger,
	hbd HeartBeatDetails,
) (HeartBeatDetails, error) {
	rateLimit := rate.Limit(config.rps)
	burstLimit := int(math.Ceil(config.rps)) // should never be zero because everything would be rejected
	rateLimiter := rate.NewLimiter(rateLimit, burstLimit)

	concurrency := int(math.Max(1, float64(config.concurrency)))

	taskCh := make(chan task, concurrency)
	respCh := make(chan taskResponse, concurrency)

	// Ticker for frequent heartbeats to avoid timeout during slow processing, 1/4 of the default heartbeat timeout (10s)
	heartbeatTicker := time.NewTicker(defaultActivityHeartBeatTimeout / 4)
	defer heartbeatTicker.Stop()

	// Start worker processors
	for range config.concurrency {
		go startWorkerProcessor(ctx, taskCh, respCh, rateLimiter, sdkClient, a.FrontendClient, metricsHandler, logger)
	}

	// Initialize the first p from initial executions or fetch from query
	var p *page
	if len(config.initialExecutions) > 0 {
		// Use initial executions
		p = &page{
			executions:    config.initialExecutions,
			nextPageToken: config.initialPageToken,
			pageNumber:    hbd.CurrentPage,
		}
	} else {
		// Fetch page of executions if needed
		var err error
		p, err = fetchPage(ctx, sdkClient, config, config.initialPageToken, hbd.CurrentPage)
		if err != nil {
			metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
			return HeartBeatDetails{}, fmt.Errorf("failed to fetch next page: %w", err)
		}
	}

	for {
		// Check if we need to fetch next page
		if p.hasNext() && p.allSubmitted() {
			nextPage, err := fetchPage(ctx, sdkClient, config, p.nextPageToken, p.pageNumber+1)
			if err != nil {
				metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
				return HeartBeatDetails{}, fmt.Errorf("failed to fetch next page: %w", err)
			}
			p.next = nextPage
			nextPage.prev = p
			p = nextPage

			hbd.CurrentPage = p.pageNumber
			hbd.PageToken = p.nextPageToken
		}

		select {
		case taskCh <- p.nextTask():
			p.submittedCount++

		case result := <-respCh:
			resultPage := result.page
			if result.err == nil {
				resultPage.successCount++
			} else {
				resultPage.errorCount++
			}

			// Update heartbeat details if this page and all previous pages are complete
			// Find all pages from the current one on that are done, record their stats, and unlink them.
			for page := resultPage; page != nil && page.done(); page = page.next {
				hbd.SuccessCount += page.successCount
				hbd.ErrorCount += page.errorCount
				page.prev = nil
			}

		case <-heartbeatTicker.C:
			// Send periodic heartbeat to prevent timeout during slow processing
			activity.RecordHeartbeat(ctx, hbd)

		case <-ctx.Done():
			metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
			logger.Error("Failed to complete batch operation", tag.Error(ctx.Err()))
			return HeartBeatDetails{}, ctx.Err()
		}

		// Check if we're done
		if p.done() && !p.hasNext() {
			break
		}
	}

	return hbd, nil
}

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

func (a *activities) checkNamespaceID(namespaceID string) error {
	if namespaceID != a.namespaceID.String() {
		return errNamespaceMismatch
	}
	return nil
}

// BatchActivity is an activity for processing batch operation.
func (a *activities) BatchActivity(ctx context.Context, batchParams BatchParams) (HeartBeatDetails, error) {
	logger := a.getActivityLogger(ctx)
	hbd := HeartBeatDetails{}
	metricsHandler := a.MetricsHandler.WithTags(metrics.OperationTag(metrics.BatcherScope), metrics.NamespaceTag(batchParams.Namespace))

	if err := a.checkNamespace(batchParams.Namespace); err != nil {
		metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
		logger.Error("Failed to run batch operation due to namespace mismatch", tag.Error(err))
		return hbd, err
	}

	// Deserialize batch reset options if set
	if b := batchParams.ResetParams.ResetOptions; b != nil {
		batchParams.ResetParams.resetOptions = &commonpb.ResetOptions{}
		if err := batchParams.ResetParams.resetOptions.Unmarshal(b); err != nil {
			logger.Error("Failed to deserialize batch reset options", tag.Error(err))
			return hbd, err
		}
	}

	// Deserialize batch post reset operations if set
	if postOps := batchParams.ResetParams.PostResetOperations; postOps != nil {
		batchParams.ResetParams.postResetOperations = make([]*workflowpb.PostResetOperation, len(postOps))
		for i, serializedOp := range postOps {
			op := &workflowpb.PostResetOperation{}
			if err := op.Unmarshal(serializedOp); err != nil {
				logger.Error("Failed to deserialize batch post reset operation", tag.Error(err))
				return hbd, err
			}
			batchParams.ResetParams.postResetOperations[i] = op
		}
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

	adjustedQuery := a.adjustQuery(batchParams.Query, batchParams.BatchType)

	if startOver {
		estimateCount := int64(len(batchParams.Executions))
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

	// Prepare configuration for shared processing function
	config := batchProcessorConfig{
		namespace:         batchParams.Namespace,
		adjustedQuery:     adjustedQuery,
		rps:               a.getOperationRPS(batchParams.RPS),
		concurrency:       a.getOperationConcurrency(batchParams.Concurrency),
		initialPageToken:  hbd.PageToken,
		initialExecutions: batchParams.Executions,
	}

	// Create a wrapper for the batch params specific worker processor
	workerProcessor := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		rateLimiter *rate.Limiter,
		sdkClient sdkclient.Client,
		frontendClient workflowservice.WorkflowServiceClient,
		metricsHandler metrics.Handler,
		logger log.Logger,
	) {
		startTaskProcessor(ctx, batchParams, taskCh, respCh, rateLimiter, sdkClient, a.FrontendClient, metricsHandler, logger)
	}

	return a.processWorkflowsWithProactiveFetching(ctx, config, workerProcessor, sdkClient, metricsHandler, logger, hbd)
}

// BatchActivityWithProtobuf is an activity for processing batch operations using protobuf as the input type.
// nolint:revive,cognitive-complexity
func (a *activities) BatchActivityWithProtobuf(ctx context.Context, batchParams *batchspb.BatchOperationInput) (HeartBeatDetails, error) {
	logger := a.getActivityLogger(ctx)
	hbd := HeartBeatDetails{}
	metricsHandler := a.MetricsHandler.WithTags(metrics.OperationTag(metrics.BatcherScope), metrics.NamespaceIDTag(batchParams.NamespaceId))

	if err := a.checkNamespaceID(batchParams.NamespaceId); err != nil {
		metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
		logger.Error("Failed to run batch operation due to namespace mismatch", tag.Error(err))
		return hbd, err
	}

	sdkClient := a.ClientFactory.NewClient(sdkclient.Options{
		Namespace:     a.namespace.String(),
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

	adjustedQuery := a.adjustQueryBatchTypeEnum(batchParams.Request.VisibilityQuery, batchParams.BatchType)

	if startOver {
		estimateCount := int64(len(batchParams.Request.Executions))
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

	// Prepare configuration for shared processing function
	config := batchProcessorConfig{
		namespace:         batchParams.Request.Namespace,
		adjustedQuery:     adjustedQuery,
		rps:               float64(a.rps(batchParams.Request.Namespace)),
		concurrency:       a.getOperationConcurrency(int(batchParams.Concurrency)),
		initialPageToken:  hbd.PageToken,
		initialExecutions: batchParams.Request.Executions,
	}

	// Create a wrapper for the protobuf specific worker processor
	workerProcessor := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		rateLimiter *rate.Limiter,
		sdkClient sdkclient.Client,
		frontendClient workflowservice.WorkflowServiceClient,
		metricsHandler metrics.Handler,
		logger log.Logger,
	) {
		startTaskProcessorProtobuf(ctx, batchParams, batchParams.Request.Namespace, taskCh, respCh, rateLimiter, sdkClient, frontendClient, metricsHandler, logger)
	}

	return a.processWorkflowsWithProactiveFetching(ctx, config, workerProcessor, sdkClient, metricsHandler, logger, hbd)
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

func (a *activities) adjustQuery(query, batchType string) string {
	if len(query) == 0 {
		// don't add anything if query is empty
		return query
	}

	switch batchType {
	case BatchTypeTerminate, BatchTypeSignal, BatchTypeCancel, BatchTypeUpdateOptions, BatchTypeUnpauseActivities:
		return fmt.Sprintf("(%s) AND (%s)", query, statusRunningQueryFilter)
	default:
		return query
	}
}

func (a *activities) adjustQueryBatchTypeEnum(query string, batchType enumspb.BatchOperationType) string {
	if len(query) == 0 {
		// don't add anything if query is empty
		return query
	}

	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE, enumspb.BATCH_OPERATION_TYPE_SIGNAL, enumspb.BATCH_OPERATION_TYPE_CANCEL, enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS, enumspb.BATCH_OPERATION_TYPE_UNPAUSE_ACTIVITY:
		return fmt.Sprintf("(%s) AND (%s)", query, statusRunningQueryFilter)
	default:
		return query
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
	batchParams BatchParams,
	taskCh chan task,
	respCh chan taskResponse,
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

			if task.execution == nil {
				continue
			}
			var err error

			switch batchParams.BatchType {
			case BatchTypeTerminate:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						return sdkClient.TerminateWorkflow(ctx, execution.WorkflowId, execution.RunId, batchParams.Reason)
					})
			case BatchTypeCancel:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						return sdkClient.CancelWorkflow(ctx, execution.WorkflowId, execution.RunId)
					})
			case BatchTypeSignal:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						_, err := frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
							Namespace:         batchParams.Namespace,
							WorkflowExecution: execution,
							SignalName:        batchParams.SignalParams.SignalName,
							Input:             batchParams.SignalParams.Input,
						})
						return err
					})
			case BatchTypeDelete:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						_, err := frontendClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
							Namespace:         batchParams.Namespace,
							WorkflowExecution: execution,
						})
						return err
					})
			case BatchTypeReset:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						var eventId int64
						var err error
						var resetReapplyType enumspb.ResetReapplyType
						var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
						if batchParams.ResetParams.resetOptions != nil {
							// Using ResetOptions
							// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
							eventId, err = getResetEventIDByOptions(ctx, batchParams.ResetParams.resetOptions, batchParams.Namespace, execution, frontendClient, logger)
							resetReapplyType = batchParams.ResetParams.resetOptions.ResetReapplyType
							resetReapplyExcludeTypes = batchParams.ResetParams.resetOptions.ResetReapplyExcludeTypes
						} else {
							// Old fields
							eventId, err = getResetEventIDByType(ctx, batchParams.ResetParams.ResetType, batchParams.Namespace, execution, frontendClient, logger)
							resetReapplyType = batchParams.ResetParams.ResetReapplyType
						}
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
							Namespace:                 batchParams.Namespace,
							WorkflowExecution:         execution,
							Reason:                    batchParams.Reason,
							RequestId:                 uuid.New(),
							WorkflowTaskFinishEventId: eventId,
							ResetReapplyType:          resetReapplyType,
							ResetReapplyExcludeTypes:  resetReapplyExcludeTypes,
							PostResetOperations:       batchParams.ResetParams.postResetOperations,
						})
						return err
					})
			case BatchTypeUnpauseActivities:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						unpauseRequest := &workflowservice.UnpauseActivityRequest{
							Namespace:      batchParams.Namespace,
							Execution:      execution,
							Identity:       batchParams.UnpauseActivitiesParams.Identity,
							Activity:       &workflowservice.UnpauseActivityRequest_Type{Type: batchParams.UnpauseActivitiesParams.ActivityType},
							ResetAttempts:  !batchParams.UnpauseActivitiesParams.ResetAttempts,
							ResetHeartbeat: batchParams.UnpauseActivitiesParams.ResetHeartbeat,
							Jitter:         durationpb.New(batchParams.UnpauseActivitiesParams.Jitter),
						}

						if batchParams.UnpauseActivitiesParams.MatchAll {
							unpauseRequest.Activity = &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true}
						} else {
							unpauseRequest.Activity = &workflowservice.UnpauseActivityRequest_Type{Type: batchParams.UnpauseActivitiesParams.ActivityType}
						}
						_, err = frontendClient.UnpauseActivity(ctx, unpauseRequest)
						return err
					})

			case BatchTypeUpdateOptions:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						var err error
						_, err = frontendClient.UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
							Namespace:                batchParams.Namespace,
							WorkflowExecution:        execution,
							WorkflowExecutionOptions: batchParams.UpdateOptionsParams.WorkflowExecutionOptions,
							UpdateMask:               &fieldmaskpb.FieldMask{Paths: batchParams.UpdateOptionsParams.UpdateMask.Paths},
						})
						return err
					})

			case BatchTypeResetActivities:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						resetRequest := &workflowservice.ResetActivityRequest{
							Namespace:              batchParams.Namespace,
							Execution:              execution,
							Identity:               batchParams.ResetActivitiesParams.Identity,
							Activity:               &workflowservice.ResetActivityRequest_Type{Type: batchParams.ResetActivitiesParams.ActivityType},
							ResetHeartbeat:         batchParams.ResetActivitiesParams.ResetHeartbeat,
							Jitter:                 durationpb.New(batchParams.ResetActivitiesParams.Jitter),
							KeepPaused:             batchParams.ResetActivitiesParams.KeepPaused,
							RestoreOriginalOptions: batchParams.ResetActivitiesParams.RestoreOriginalOptions,
						}

						if batchParams.ResetActivitiesParams.MatchAll {
							resetRequest.Activity = &workflowservice.ResetActivityRequest_MatchAll{MatchAll: true}
						} else {
							resetRequest.Activity = &workflowservice.ResetActivityRequest_Type{Type: batchParams.ResetActivitiesParams.ActivityType}
						}

						_, err = frontendClient.ResetActivity(ctx, resetRequest)
						return err
					})
			case BatchTypeUpdateActivitiesOptions:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						updateRequest := &workflowservice.UpdateActivityOptionsRequest{
							Namespace:       batchParams.Namespace,
							Execution:       execution,
							Activity:        &workflowservice.UpdateActivityOptionsRequest_Type{Type: batchParams.UpdateActivitiesOptionsParams.ActivityType},
							UpdateMask:      &fieldmaskpb.FieldMask{Paths: batchParams.UpdateActivitiesOptionsParams.UpdateMask.Paths},
							RestoreOriginal: batchParams.UpdateActivitiesOptionsParams.RestoreOriginal,
							Identity:        batchParams.UpdateActivitiesOptionsParams.Identity,
						}

						if ao := batchParams.UpdateActivitiesOptionsParams.ActivityOptions; ao != nil {
							updateRequest.ActivityOptions = &activitypb.ActivityOptions{
								ScheduleToStartTimeout: durationpb.New(ao.ScheduleToStartTimeout),
								ScheduleToCloseTimeout: durationpb.New(ao.ScheduleToCloseTime),
								StartToCloseTimeout:    durationpb.New(ao.StartToCloseTimeout),
								HeartbeatTimeout:       durationpb.New(ao.HeartbeatTimeout),
							}

							if rp := ao.RetryPolicy; rp != nil {
								updateRequest.ActivityOptions.RetryPolicy = &commonpb.RetryPolicy{
									InitialInterval:        durationpb.New(rp.InitialInterval),
									BackoffCoefficient:     rp.BackoffCoefficient,
									MaximumInterval:        durationpb.New(rp.MaximumInterval),
									MaximumAttempts:        rp.MaximumAttempts,
									NonRetryableErrorTypes: rp.NonRetryableErrorTypes,
								}
							}
						}

						if batchParams.UpdateActivitiesOptionsParams.MatchAll {
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_MatchAll{MatchAll: true}
						} else {
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Type{Type: batchParams.UpdateActivitiesOptionsParams.ActivityType}
						}

						_, err = frontendClient.UpdateActivityOptions(ctx, updateRequest)
						return err
					})
			default:
				err = errors.New("unknown batch type: " + batchParams.BatchType)
			}
			if err != nil {
				metrics.BatcherProcessorFailures.With(metricsHandler).Record(1)
				logger.Error("Failed to process batch operation task", tag.Error(err))

				_, ok := batchParams._nonRetryableErrors[err.Error()]
				if ok || task.attempts > batchParams.AttemptsOnRetryableError {
					respCh <- taskResponse{err: err, page: task.page}
				} else {
					// put back to the channel if less than attemptsOnError
					task.attempts++
					taskCh <- task
				}
			} else {
				metrics.BatcherProcessorSuccess.With(metricsHandler).Record(1)
				respCh <- taskResponse{err: nil, page: task.page}
			}
		}
	}
}

// nolint:revive,cognitive-complexity
func startTaskProcessorProtobuf(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	namespace string,
	taskCh chan task,
	respCh chan taskResponse,
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

			if task.execution == nil {
				continue
			}

			switch operation := batchOperation.Request.Operation.(type) {
			case *workflowservice.StartBatchOperationRequest_TerminationOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						return sdkClient.TerminateWorkflow(ctx, execution.WorkflowId, execution.RunId, batchOperation.Request.Reason)
					})
			case *workflowservice.StartBatchOperationRequest_CancellationOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						return sdkClient.CancelWorkflow(ctx, execution.WorkflowId, execution.RunId)
					})
			case *workflowservice.StartBatchOperationRequest_SignalOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						_, err := frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
							Namespace:         namespace,
							WorkflowExecution: execution,
							SignalName:        operation.SignalOperation.GetSignal(),
							Input:             operation.SignalOperation.GetInput(),
							Identity:          operation.SignalOperation.GetIdentity(),
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_DeletionOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						_, err := frontendClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
							Namespace:         namespace,
							WorkflowExecution: execution,
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_ResetOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						var eventId int64
						var err error
						//nolint:staticcheck // SA1019: worker versioning v0.31
						var resetReapplyType enumspb.ResetReapplyType
						var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
						if operation.ResetOperation.Options != nil {
							// Using ResetOptions
							// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByOptions(ctx, operation.ResetOperation.Options, namespace, execution, frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = operation.ResetOperation.Options.ResetReapplyType
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyExcludeTypes = operation.ResetOperation.Options.ResetReapplyExcludeTypes
						} else {
							// Old fields
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByType(ctx, operation.ResetOperation.ResetType, batchOperation.Request.Namespace, execution, frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = operation.ResetOperation.ResetReapplyType
						}
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
							Namespace:                 namespace,
							WorkflowExecution:         execution,
							Reason:                    batchOperation.Request.Reason,
							RequestId:                 uuid.New(),
							WorkflowTaskFinishEventId: eventId,
							ResetReapplyType:          resetReapplyType,
							ResetReapplyExcludeTypes:  resetReapplyExcludeTypes,
							PostResetOperations:       operation.ResetOperation.PostResetOperations,
							Identity:                  operation.ResetOperation.Identity,
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						unpauseRequest := &workflowservice.UnpauseActivityRequest{
							Namespace:      namespace,
							Execution:      execution,
							Identity:       operation.UnpauseActivitiesOperation.Identity,
							ResetAttempts:  operation.UnpauseActivitiesOperation.ResetAttempts,
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
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", operation.UnpauseActivitiesOperation.GetActivity()))
						}

						_, err = frontendClient.UnpauseActivity(ctx, unpauseRequest)
						return err
					})

			case *workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						var err error
						_, err = frontendClient.UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
							Namespace:                namespace,
							WorkflowExecution:        execution,
							WorkflowExecutionOptions: operation.UpdateWorkflowOptionsOperation.WorkflowExecutionOptions,
							UpdateMask:               &fieldmaskpb.FieldMask{Paths: operation.UpdateWorkflowOptionsOperation.UpdateMask.Paths},
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_ResetActivitiesOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						resetRequest := &workflowservice.ResetActivityRequest{
							Namespace:              namespace,
							Execution:              execution,
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
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", operation.ResetActivitiesOperation.GetActivity()))
						}

						_, err = frontendClient.ResetActivity(ctx, resetRequest)
						return err
					})
			case *workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation:
				err = processTask(ctx, limiter, task,
					func(execution *commonpb.WorkflowExecution) error {
						updateRequest := &workflowservice.UpdateActivityOptionsRequest{
							Namespace:       namespace,
							Execution:       execution,
							UpdateMask:      &fieldmaskpb.FieldMask{Paths: operation.UpdateActivityOptionsOperation.UpdateMask.Paths},
							RestoreOriginal: operation.UpdateActivityOptionsOperation.RestoreOriginal,
							Identity:        operation.UpdateActivityOptionsOperation.Identity,
						}

						switch ao := operation.UpdateActivityOptionsOperation.GetActivity().(type) {
						case *batchpb.BatchOperationUpdateActivityOptions_Type:
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Type{Type: ao.Type}
						case *batchpb.BatchOperationUpdateActivityOptions_MatchAll:
							updateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_MatchAll{MatchAll: true}
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", operation.UpdateActivityOptionsOperation.GetActivity()))
						}

						updateRequest.ActivityOptions = operation.UpdateActivityOptionsOperation.GetActivityOptions()
						_, err = frontendClient.UpdateActivityOptions(ctx, updateRequest)
						return err
					})
			default:
				err = errors.New(fmt.Sprintf("unknown batch type: %v", batchOperation.BatchType))
			}
			if err != nil {
				metrics.BatcherProcessorFailures.With(metricsHandler).Record(1)
				logger.Error("Failed to process batch operation task", tag.Error(err))
				nonRetryable := slices.Contains(batchOperation.NonRetryableErrors, err.Error())
				if nonRetryable || task.attempts > int(batchOperation.AttemptsOnRetryableError) {
					respCh <- taskResponse{err: err, page: task.page}
				} else {
					// put back to the channel if less than attemptsOnError
					task.attempts++
					taskCh <- task
				}
			} else {
				metrics.BatcherProcessorSuccess.With(metricsHandler).Record(1)
				respCh <- taskResponse{err: nil, page: task.page}
			}
		}
	}
}

func processTask(
	ctx context.Context,
	limiter *rate.Limiter,
	task task,
	procFn func(*commonpb.WorkflowExecution) error,
) error {
	err := limiter.Wait(ctx)
	if err != nil {
		return err
	}

	err = procFn(task.execution)
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
