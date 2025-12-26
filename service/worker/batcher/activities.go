package batcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
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
	executionInfos []*workflowpb.WorkflowExecutionInfo
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
	return p.submittedCount == len(p.executionInfos)
}

// nextTask returns the next task to be submitted from this page
func (p *page) nextTask() task {
	if p.submittedCount >= len(p.executionInfos) {
		return task{} // No more tasks in this page
	}

	task := task{
		executionInfo: p.executionInfos[p.submittedCount],
		attempts:      1,
		page:          p,
	}
	return task
}

// done returns true if this page and all previous pages are complete
func (p *page) done() bool {
	if p.prev != nil && !p.prev.done() {
		return false
	}
	return p.successCount+p.errorCount == len(p.executionInfos)
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
			executionInfos: []*workflowpb.WorkflowExecutionInfo{},
			nextPageToken:  []byte{},
			pageNumber:     pageNumber,
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

	executionInfos := make([]*workflowpb.WorkflowExecutionInfo, 0, len(resp.Executions))
	for _, wf := range resp.Executions {
		executionInfos = append(executionInfos, wf)
	}

	return &page{
		executionInfos: executionInfos,
		nextPageToken:  resp.NextPageToken,
		pageNumber:     pageNumber,
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
		// Use initial executions - convert WorkflowExecution to WorkflowExecutionInfo
		executionInfos := make([]*workflowpb.WorkflowExecutionInfo, 0, len(config.initialExecutions))
		for _, exec := range config.initialExecutions {
			executionInfos = append(executionInfos, &workflowpb.WorkflowExecutionInfo{
				Execution: exec,
			})
		}

		p = &page{
			executionInfos: executionInfos,
			nextPageToken:  config.initialPageToken,
			pageNumber:     hbd.CurrentPage,
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

func (a *activities) checkNamespaceID(namespaceID string) error {
	if namespaceID != a.namespaceID.String() {
		return errNamespaceMismatch
	}
	return nil
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

	// Get namespace and query based on request type (public vs admin)
	var ns string
	var visibilityQuery string
	var executions []*commonpb.WorkflowExecution

	if batchParams.AdminRequest != nil {
		ctx = headers.SetCallerType(ctx, headers.CallerTypePreemptable)
		adminReq := batchParams.AdminRequest
		ns = adminReq.Namespace
		visibilityQuery = adminReq.GetVisibilityQuery()
		executions = adminReq.GetExecutions()
	} else {
		ns = batchParams.Request.Namespace
		visibilityQuery = a.adjustQueryBatchTypeEnum(batchParams.Request.VisibilityQuery, batchParams.BatchType)
		executions = batchParams.Request.Executions
	}

	if startOver {
		estimateCount := int64(len(executions))
		if len(visibilityQuery) > 0 {
			resp, err := sdkClient.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
				Query: visibilityQuery,
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
		namespace:         ns,
		adjustedQuery:     visibilityQuery,
		rps:               float64(a.rps(ns)),
		concurrency:       a.getOperationConcurrency(int(batchParams.Concurrency)),
		initialPageToken:  hbd.PageToken,
		initialExecutions: executions,
	}

	// Create a wrapper for the task processor
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
		a.startTaskProcessor(ctx, batchParams, ns, taskCh, respCh, rateLimiter, sdkClient, frontendClient, metricsHandler, logger)
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

func (a *activities) adjustQueryBatchTypeEnum(query string, batchType enumspb.BatchOperationType) string {
	if len(query) == 0 {
		// don't add anything if query is empty
		return query
	}

	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE, enumspb.BATCH_OPERATION_TYPE_SIGNAL, enumspb.BATCH_OPERATION_TYPE_CANCEL, enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS, enumspb.BATCH_OPERATION_TYPE_UNPAUSE_ACTIVITY, enumspb.BATCH_OPERATION_TYPE_UPDATE_ACTIVITY_OPTIONS, enumspb.BATCH_OPERATION_TYPE_RESET_ACTIVITY:
		return fmt.Sprintf("(%s) AND (%s)", query, statusRunningQueryFilter)
	default:
		return query
	}
}

func (a *activities) adjustQueryAdminBatchType(adminReq *adminservice.StartAdminBatchOperationRequest) string {
	// RefreshWorkflowTasks applies to both open and closed workflows,
	// so no additional filter is needed - return query as-is.
	return adminReq.GetVisibilityQuery()
}

func (a *activities) getOperationConcurrency(concurrency int) int {
	if concurrency <= 0 {
		return a.concurrency(a.namespace.String())
	}
	return concurrency
}

// nolint:revive,cognitive-complexity
func (a *activities) startTaskProcessor(
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

			if task.executionInfo == nil {
				continue
			}

			// Handle admin batch operations
			if batchOperation.AdminRequest != nil {
				err = a.processAdminTask(ctx, batchOperation, task, limiter)
				a.handleTaskResult(batchOperation, task, err, taskCh, respCh, metricsHandler, logger)
				continue
			}

			switch operation := batchOperation.Request.Operation.(type) {
			case *workflowservice.StartBatchOperationRequest_TerminationOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						return sdkClient.TerminateWorkflow(ctx, executionInfo.Execution.WorkflowId, executionInfo.Execution.RunId, batchOperation.Request.Reason)
					})
			case *workflowservice.StartBatchOperationRequest_CancellationOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						return sdkClient.CancelWorkflow(ctx, executionInfo.Execution.WorkflowId, executionInfo.Execution.RunId)
					})
			case *workflowservice.StartBatchOperationRequest_SignalOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						_, err = frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
							Namespace:         namespace,
							WorkflowExecution: executionInfo.Execution,
							SignalName:        operation.SignalOperation.GetSignal(),
							Input:             operation.SignalOperation.GetInput(),
							Identity:          operation.SignalOperation.GetIdentity(),
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_DeletionOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						_, err := frontendClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
							Namespace:         namespace,
							WorkflowExecution: executionInfo.Execution,
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_ResetOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						var eventId int64
						var err error
						//nolint:staticcheck // SA1019: worker versioning v0.31
						var resetReapplyType enumspb.ResetReapplyType
						var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
						if operation.ResetOperation.Options != nil {
							// Using ResetOptions
							// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByOptions(ctx, operation.ResetOperation.Options, namespace, executionInfo.Execution, frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = operation.ResetOperation.Options.ResetReapplyType
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyExcludeTypes = operation.ResetOperation.Options.ResetReapplyExcludeTypes
						} else {
							// Old fields
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByType(ctx, operation.ResetOperation.ResetType, batchOperation.Request.Namespace, executionInfo.Execution, frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = operation.ResetOperation.ResetReapplyType
						}
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
							Namespace:                 namespace,
							WorkflowExecution:         executionInfo.Execution,
							Reason:                    batchOperation.Request.Reason,
							RequestId:                 uuid.NewString(),
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
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						unpauseRequest := &workflowservice.UnpauseActivityRequest{
							Namespace:      namespace,
							Execution:      executionInfo.Execution,
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
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						var err error
						_, err = frontendClient.UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
							Namespace:                namespace,
							WorkflowExecution:        executionInfo.Execution,
							WorkflowExecutionOptions: operation.UpdateWorkflowOptionsOperation.WorkflowExecutionOptions,
							UpdateMask:               &fieldmaskpb.FieldMask{Paths: operation.UpdateWorkflowOptionsOperation.UpdateMask.Paths},
							Identity:                 operation.UpdateWorkflowOptionsOperation.Identity,
						})
						return err
					})
			case *workflowservice.StartBatchOperationRequest_ResetActivitiesOperation:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						resetRequest := &workflowservice.ResetActivityRequest{
							Namespace:              namespace,
							Execution:              executionInfo.Execution,
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
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						updateRequest := &workflowservice.UpdateActivityOptionsRequest{
							Namespace:       namespace,
							Execution:       executionInfo.Execution,
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
			a.handleTaskResult(batchOperation, task, err, taskCh, respCh, metricsHandler, logger)
		}
	}
}

func (a *activities) handleTaskResult(
	batchOperation *batchspb.BatchOperationInput,
	task task,
	err error,
	taskCh chan task,
	respCh chan taskResponse,
	metricsHandler metrics.Handler,
	logger log.Logger,
) {
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

func (a *activities) processAdminTask(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	task task,
	limiter *rate.Limiter,
) error {
	adminReq := batchOperation.AdminRequest
	switch adminReq.Operation.(type) {
	case *adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation:
		return processTask(ctx, limiter, task,
			func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
				archetypeID, err := workercommon.ArchetypeIDFromExecutionInfo(executionInfo)
				if err != nil {
					return fmt.Errorf("archetypeID extraction error: %w", err)
				}
				_, err = a.HistoryClient.RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
					NamespaceId: batchOperation.NamespaceId,
					ArchetypeId: uint32(archetypeID),
					Request: &adminservice.RefreshWorkflowTasksRequest{
						NamespaceId: batchOperation.NamespaceId,
						Execution:   executionInfo.Execution,
					},
				})
				return err
			})
	default:
		return fmt.Errorf("unknown admin batch type: %T", adminReq.Operation)
	}
}

func processTask(
	ctx context.Context,
	limiter *rate.Limiter,
	task task,
	procFn func(*workflowpb.WorkflowExecutionInfo) error,
) error {
	err := limiter.Wait(ctx)
	if err != nil {
		return err
	}

	err = procFn(task.executionInfo)
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
