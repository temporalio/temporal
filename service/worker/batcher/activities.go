package batcher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
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
	"go.temporal.io/sdk/temporal"
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
	"go.temporal.io/server/common/worker_versioning"
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

	resp, err := sdkClient.ListWorkflow(ctx, workflowservice.ListWorkflowExecutionsRequest_builder{
		PageSize:      int32(pageSize),
		NextPageToken: pageToken,
		Query:         config.adjustedQuery,
	}.Build())
	if err != nil {
		return nil, err
	}

	executionInfos := make([]*workflowpb.WorkflowExecutionInfo, 0, len(resp.GetExecutions()))
	for _, wf := range resp.GetExecutions() {
		executionInfos = append(executionInfos, wf)
	}

	return &page{
		executionInfos: executionInfos,
		nextPageToken:  resp.GetNextPageToken(),
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
			executionInfos = append(executionInfos, workflowpb.WorkflowExecutionInfo_builder{
				Execution: exec,
			}.Build())
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
	metricsHandler := a.MetricsHandler.WithTags(metrics.OperationTag(metrics.BatcherScope), metrics.NamespaceIDTag(batchParams.GetNamespaceId()))

	if err := a.checkNamespaceID(batchParams.GetNamespaceId()); err != nil {
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

	if batchParams.HasAdminRequest() {
		ctx = headers.SetCallerType(ctx, headers.CallerTypePreemptable)
		adminReq := batchParams.GetAdminRequest()
		ns = adminReq.GetNamespace()
		visibilityQuery = adminReq.GetVisibilityQuery()
		executions = adminReq.GetExecutions()
	} else {
		ns = batchParams.GetRequest().GetNamespace()
		visibilityQuery = a.adjustQueryBatchTypeEnum(batchParams.GetRequest().GetVisibilityQuery(), batchParams.GetBatchType())
		executions = batchParams.GetRequest().GetExecutions()
	}

	if startOver {
		estimateCount := int64(len(executions))
		if len(visibilityQuery) > 0 {
			resp, err := sdkClient.CountWorkflow(ctx, workflowservice.CountWorkflowExecutionsRequest_builder{
				Query: visibilityQuery,
			}.Build())
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
		concurrency:       a.getOperationConcurrency(int(batchParams.GetConcurrency())),
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
			if batchOperation.HasAdminRequest() {
				err = a.processAdminTask(ctx, batchOperation, task, limiter)
				a.handleTaskResult(batchOperation, task, err, taskCh, respCh, metricsHandler, logger)
				continue
			}

			switch batchOperation.GetRequest().WhichOperation() {
			case workflowservice.StartBatchOperationRequest_TerminationOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						return sdkClient.TerminateWorkflow(ctx, executionInfo.GetExecution().GetWorkflowId(), executionInfo.GetExecution().GetRunId(), batchOperation.GetRequest().GetReason())
					})
			case workflowservice.StartBatchOperationRequest_CancellationOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						return sdkClient.CancelWorkflow(ctx, executionInfo.GetExecution().GetWorkflowId(), executionInfo.GetExecution().GetRunId())
					})
			case workflowservice.StartBatchOperationRequest_SignalOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						_, err = frontendClient.SignalWorkflowExecution(ctx, workflowservice.SignalWorkflowExecutionRequest_builder{
							Namespace:         namespace,
							WorkflowExecution: executionInfo.GetExecution(),
							SignalName:        batchOperation.GetRequest().GetSignalOperation().GetSignal(),
							Input:             batchOperation.GetRequest().GetSignalOperation().GetInput(),
							Identity:          batchOperation.GetRequest().GetSignalOperation().GetIdentity(),
						}.Build())
						return err
					})
			case workflowservice.StartBatchOperationRequest_DeletionOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						_, err := frontendClient.DeleteWorkflowExecution(ctx, workflowservice.DeleteWorkflowExecutionRequest_builder{
							Namespace:         namespace,
							WorkflowExecution: executionInfo.GetExecution(),
						}.Build())
						return err
					})
			case workflowservice.StartBatchOperationRequest_ResetOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						var eventId int64
						var err error
						//nolint:staticcheck // SA1019: worker versioning v0.31
						var resetReapplyType enumspb.ResetReapplyType
						var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
						if batchOperation.GetRequest().GetResetOperation().HasOptions() {
							// Using ResetOptions
							// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByOptions(ctx, batchOperation.GetRequest().GetResetOperation().GetOptions(), namespace, executionInfo.GetExecution(), frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = batchOperation.GetRequest().GetResetOperation().GetOptions().GetResetReapplyType()
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyExcludeTypes = batchOperation.GetRequest().GetResetOperation().GetOptions().GetResetReapplyExcludeTypes()
						} else {
							// Old fields
							//nolint:staticcheck // SA1019: worker versioning v0.31
							eventId, err = getResetEventIDByType(ctx, batchOperation.GetRequest().GetResetOperation().GetResetType(), batchOperation.GetRequest().GetNamespace(), executionInfo.GetExecution(), frontendClient, logger)
							//nolint:staticcheck // SA1019: worker versioning v0.31
							resetReapplyType = batchOperation.GetRequest().GetResetOperation().GetResetReapplyType()
						}
						if err != nil {
							return err
						}
						_, err = frontendClient.ResetWorkflowExecution(ctx, workflowservice.ResetWorkflowExecutionRequest_builder{
							Namespace:                 namespace,
							WorkflowExecution:         executionInfo.GetExecution(),
							Reason:                    batchOperation.GetRequest().GetReason(),
							RequestId:                 uuid.NewString(),
							WorkflowTaskFinishEventId: eventId,
							ResetReapplyType:          resetReapplyType,
							ResetReapplyExcludeTypes:  resetReapplyExcludeTypes,
							PostResetOperations:       batchOperation.GetRequest().GetResetOperation().GetPostResetOperations(),
							Identity:                  batchOperation.GetRequest().GetResetOperation().GetIdentity(),
						}.Build())
						return err
					})
			case workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						unpauseRequest := workflowservice.UnpauseActivityRequest_builder{
							Namespace:      namespace,
							Execution:      executionInfo.GetExecution(),
							Identity:       batchOperation.GetRequest().GetUnpauseActivitiesOperation().GetIdentity(),
							ResetAttempts:  batchOperation.GetRequest().GetUnpauseActivitiesOperation().GetResetAttempts(),
							ResetHeartbeat: batchOperation.GetRequest().GetUnpauseActivitiesOperation().GetResetHeartbeat(),
							Jitter:         batchOperation.GetRequest().GetUnpauseActivitiesOperation().GetJitter(),
						}.Build()

						switch batchOperation.GetRequest().GetUnpauseActivitiesOperation().WhichActivity() {
						case batchpb.BatchOperationUnpauseActivities_Type_case:
							unpauseRequest.SetType(batchOperation.GetRequest().GetUnpauseActivitiesOperation().GetType())
						case batchpb.BatchOperationUnpauseActivities_MatchAll_case:
							unpauseRequest.SetUnpauseAll(true)
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", batchOperation.GetRequest().GetUnpauseActivitiesOperation().WhichActivity()))
						}

						_, err = frontendClient.UnpauseActivity(ctx, unpauseRequest)
						return err
					})

			case workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						var err error
						_, err = frontendClient.UpdateWorkflowExecutionOptions(ctx, workflowservice.UpdateWorkflowExecutionOptionsRequest_builder{
							Namespace:                namespace,
							WorkflowExecution:        executionInfo.GetExecution(),
							WorkflowExecutionOptions: batchOperation.GetRequest().GetUpdateWorkflowOptionsOperation().GetWorkflowExecutionOptions(),
							UpdateMask:               &fieldmaskpb.FieldMask{Paths: batchOperation.GetRequest().GetUpdateWorkflowOptionsOperation().GetUpdateMask().Paths},
							Identity:                 batchOperation.GetRequest().GetUpdateWorkflowOptionsOperation().GetIdentity(),
						}.Build())
						return err
					})
			case workflowservice.StartBatchOperationRequest_ResetActivitiesOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						resetRequest := workflowservice.ResetActivityRequest_builder{
							Namespace:              namespace,
							Execution:              executionInfo.GetExecution(),
							Identity:               batchOperation.GetRequest().GetResetActivitiesOperation().GetIdentity(),
							ResetHeartbeat:         batchOperation.GetRequest().GetResetActivitiesOperation().GetResetHeartbeat(),
							Jitter:                 batchOperation.GetRequest().GetResetActivitiesOperation().GetJitter(),
							KeepPaused:             batchOperation.GetRequest().GetResetActivitiesOperation().GetKeepPaused(),
							RestoreOriginalOptions: batchOperation.GetRequest().GetResetActivitiesOperation().GetRestoreOriginalOptions(),
						}.Build()

						switch batchOperation.GetRequest().GetResetActivitiesOperation().WhichActivity() {
						case batchpb.BatchOperationResetActivities_Type_case:
							resetRequest.SetType(batchOperation.GetRequest().GetResetActivitiesOperation().GetType())
						case batchpb.BatchOperationResetActivities_MatchAll_case:
							resetRequest.SetMatchAll(true)
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", batchOperation.GetRequest().GetResetActivitiesOperation().WhichActivity()))
						}

						_, err = frontendClient.ResetActivity(ctx, resetRequest)
						return err
					})
			case workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation_case:
				err = processTask(ctx, limiter, task,
					func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
						updateRequest := workflowservice.UpdateActivityOptionsRequest_builder{
							Namespace:       namespace,
							Execution:       executionInfo.GetExecution(),
							UpdateMask:      &fieldmaskpb.FieldMask{Paths: batchOperation.GetRequest().GetUpdateActivityOptionsOperation().GetUpdateMask().Paths},
							RestoreOriginal: batchOperation.GetRequest().GetUpdateActivityOptionsOperation().GetRestoreOriginal(),
							Identity:        batchOperation.GetRequest().GetUpdateActivityOptionsOperation().GetIdentity(),
						}.Build()

						switch batchOperation.GetRequest().GetUpdateActivityOptionsOperation().WhichActivity() {
						case batchpb.BatchOperationUpdateActivityOptions_Type_case:
							updateRequest.SetType(batchOperation.GetRequest().GetUpdateActivityOptionsOperation().GetType())
						case batchpb.BatchOperationUpdateActivityOptions_MatchAll_case:
							updateRequest.SetMatchAll(true)
						default:
							return errors.New(fmt.Sprintf("unknown activity type: %v", batchOperation.GetRequest().GetUpdateActivityOptionsOperation().WhichActivity()))
						}

						updateRequest.SetActivityOptions(batchOperation.GetRequest().GetUpdateActivityOptionsOperation().GetActivityOptions())
						_, err = frontendClient.UpdateActivityOptions(ctx, updateRequest)
						return err
					})
			default:
				err = errors.New(fmt.Sprintf("unknown batch type: %v", batchOperation.GetBatchType()))
			}
			a.handleTaskResult(batchOperation, task, err, taskCh, respCh, metricsHandler, logger)
		}
	}
}

// isNonRetryableError determines if an error should not be retried based on the operation type
func isNonRetryableError(err error, batchType enumspb.BatchOperationType) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Operation-specific non-retryable errors
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS:
		// Pinned version that is not present in a task queue error is non-retryable for workflow options updates
		return strings.Contains(errMsg, worker_versioning.ErrPinnedVersionNotInTaskQueueSubstring)
	default:
		return false
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

		// Check if error is non-retryable:
		// 1. ApplicationError marked as NonRetryable
		// 2. Operation-specific non-retryable errors
		// 3. List of non-retryable errors from frontend
		var appErr *temporal.ApplicationError
		nonRetryable := (errors.As(err, &appErr) && appErr.NonRetryable()) ||
			isNonRetryableError(err, batchOperation.GetBatchType()) ||
			slices.Contains(batchOperation.GetNonRetryableErrors(), err.Error())

		if nonRetryable || task.attempts > int(batchOperation.GetAttemptsOnRetryableError()) {
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
	adminReq := batchOperation.GetAdminRequest()
	switch adminReq.WhichOperation() {
	case adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation_case:
		return processTask(ctx, limiter, task,
			func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
				archetypeID, err := workercommon.ArchetypeIDFromExecutionInfo(executionInfo)
				if err != nil {
					return fmt.Errorf("archetypeID extraction error: %w", err)
				}
				_, err = a.HistoryClient.RefreshWorkflowTasks(ctx, historyservice.RefreshWorkflowTasksRequest_builder{
					NamespaceId: batchOperation.GetNamespaceId(),
					ArchetypeId: uint32(archetypeID),
					Request: adminservice.RefreshWorkflowTasksRequest_builder{
						NamespaceId: batchOperation.GetNamespaceId(),
						Execution:   executionInfo.GetExecution(),
					}.Build(),
				}.Build())
				return err
			})
	default:
		return fmt.Errorf("unknown admin batch type: %v", adminReq.WhichOperation())
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
	switch resetOptions.WhichTarget() {
	case commonpb.ResetOptions_FirstWorkflowTask_case:
		return getFirstWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	case commonpb.ResetOptions_LastWorkflowTask_case:
		return getLastWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, frontendClient, logger)
	case commonpb.ResetOptions_WorkflowTaskId_case:
		return resetOptions.GetWorkflowTaskId(), nil
	case commonpb.ResetOptions_BuildId_case:
		return getResetPoint(ctx, namespaceStr, workflowExecution, frontendClient, resetOptions.GetBuildId(), resetOptions.GetCurrentRunOnly())
	default:
		errorMsg := fmt.Sprintf("provided reset target (%vv) is not supported.", resetOptions.WhichTarget())
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
	req := workflowservice.GetWorkflowExecutionHistoryReverseRequest_builder{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}.Build()
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
		if len(resp.GetNextPageToken()) == 0 {
			break
		}
		req.SetNextPageToken(resp.GetNextPageToken())
	}
	if workflowTaskEventID == 0 {
		return 0, temporal.NewNonRetryableApplicationError("unable to find any scheduled or completed task", "NoWorkflowTaskFound", nil)
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
	req := workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace:       namespaceStr,
		Execution:       workflowExecution,
		MaximumPageSize: 1000,
		NextPageToken:   nil,
	}.Build()
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
		if len(resp.GetNextPageToken()) == 0 {
			break
		}
		req.SetNextPageToken(resp.GetNextPageToken())
	}
	if workflowTaskEventID == 0 {
		return 0, temporal.NewNonRetryableApplicationError("unable to find any scheduled or completed task", "NoWorkflowTaskFound", nil)
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
	res, err := frontendClient.DescribeWorkflowExecution(ctx, workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: namespaceStr,
		Execution: execution,
	}.Build())
	if err != nil {
		return 0, err
	}
	resetPoints := res.GetWorkflowExecutionInfo().GetAutoResetPoints().GetPoints()
	for _, point := range resetPoints {
		if point.GetBuildId() == buildId {
			if !point.GetResettable() {
				return 0, fmt.Errorf("Reset point for %v is not resettable", buildId)
			} else if point.HasExpireTime() && point.GetExpireTime().AsTime().Before(time.Now()) {
				return 0, fmt.Errorf("Reset point for %v is expired", buildId)
			} else if execution.GetRunId() != point.GetRunId() && currentRunOnly {
				return 0, fmt.Errorf("Reset point for %v points to previous run and CurrentRunOnly is set", buildId)
			}
			execution.SetRunId(point.GetRunId())
			return point.GetFirstWorkflowTaskCompletedId(), nil
		}
	}
	return 0, fmt.Errorf("Can't find reset point for %v", buildId)
}
