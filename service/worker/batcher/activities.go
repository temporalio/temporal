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
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
	workercommon "go.temporal.io/server/service/worker/common"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	pageSize                 = 1000
	statusRunningQueryFilter = "ExecutionStatus='Running'"

	// defaultTaskTimeout bounds how long processing a single task may take so
	// that one hung operation cannot block the task processor forever.
	defaultTaskTimeout = 30 * time.Second
)

var (
	errNamespaceMismatch = errors.New("namespace mismatch")

	batchQuotaRequest = quotas.Request{
		Token: 1,
	}
)

// batchProcessorConfig holds the configuration for batch processing
type batchProcessorConfig struct {
	namespace         string
	adjustedQuery     string
	batchType         enumspb.BatchOperationType
	concurrency       int
	initialPageToken  []byte
	initialExecutions []*commonpb.WorkflowExecution
	// initialTargetExecutions holds an explicit list of activity target
	// executions to process, set instead of initialExecutions when the request
	// targets activity executions directly rather than via a visibility query.
	initialTargetExecutions []*commonpb.Execution
}

// batchWorkerProcessor defines the interface for different worker processor types
type batchWorkerProcessor func(
	ctx context.Context,
	taskCh chan task,
	respCh chan taskResponse,
	rateLimiter quotas.RequestRateLimiter,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
)

// page represents a page of workflow executions to be processed
type page struct {
	// executionInfos holds workflow executions for workflow batch operations;
	// targetExecutionInfo holds activity executions for activity batch
	// operations (terminate/cancel/delete activities). Exactly one is populated.
	executionInfos      []*workflowpb.WorkflowExecutionInfo
	targetExecutionInfo []*commonpb.Execution
	submittedCount      int
	successCount        int
	errorCount          int
	nextPageToken       []byte
	pageNumber          int
	prev, next          *page
}

// hasNext returns true if there are more pages to fetch
func (p *page) hasNext() bool {
	return len(p.nextPageToken) > 0
}

// length returns the number of executions in this page. For activity batch
// operations the executions are held in targetExecutionInfo; for all other
// batch types they are held in executionInfos. Only one of the two is ever
// populated for a given page.
func (p *page) length() int {
	if len(p.targetExecutionInfo) > 0 {
		return len(p.targetExecutionInfo)
	}
	return len(p.executionInfos)
}

// allSubmitted returns true if all executions in this page have been submitted
func (p *page) allSubmitted() bool {
	return p.submittedCount == p.length()
}

// nextTask returns the next task to be submitted from this page
func (p *page) nextTask() task {
	if p.submittedCount >= p.length() {
		return task{} // No more tasks in this page
	}

	t := task{
		attempts: 1,
		page:     p,
	}
	if len(p.targetExecutionInfo) > 0 {
		t.targetExecution = p.targetExecutionInfo[p.submittedCount]
	} else {
		t.executionInfo = p.executionInfos[p.submittedCount]
	}
	return t
}

// done returns true if this page and all previous pages are complete
func (p *page) done() bool {
	if p.prev != nil && !p.prev.done() {
		return false
	}
	return p.successCount+p.errorCount == p.length()
}

// recordCompletedPages records stats for each page that is now fully done and advances the
// heartbeat resume point to the oldest page that is not yet done.
func recordCompletedPages(hbd *HeartBeatDetails, resultPage *page) {
	for pg := resultPage; pg != nil && pg.done(); pg = pg.next {
		hbd.SuccessCount += pg.successCount
		hbd.ErrorCount += pg.errorCount
		hbd.CurrentPage = pg.pageNumber + 1
		hbd.PageToken = pg.nextPageToken
		pg.prev = nil
	}
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

	// Terminate/Cancel/Delete Activities batch types operate on activity executions,
	// so they are listed via ListActivityExecutions; all other batch types list workflow executions.
	var executionInfos []*workflowpb.WorkflowExecutionInfo
	var targetExecutionInfo []*commonpb.Execution
	var nextPageToken []byte
	if isActivityBatchType(config.batchType) {
		resp, err := sdkClient.WorkflowService().ListActivityExecutions(ctx,
			&workflowservice.ListActivityExecutionsRequest{
				Namespace:     config.namespace,
				PageSize:      int32(pageSize),
				NextPageToken: pageToken,
				Query:         config.adjustedQuery,
			})
		if err != nil {
			var invalidArgErr *serviceerror.InvalidArgument
			if errors.As(err, &invalidArgErr) {
				return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", err)
			}
			return nil, err
		}

		targetExecutionInfo = make([]*commonpb.Execution, 0, len(resp.GetExecutions()))
		for _, activityExecution := range resp.GetExecutions() {
			targetExecutionInfo = append(targetExecutionInfo, &commonpb.Execution{
				Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
				BusinessId: activityExecution.GetActivityId(),
				RunId:      activityExecution.GetRunId(),
			})
		}
		nextPageToken = resp.NextPageToken
	} else {
		resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      int32(pageSize),
			NextPageToken: pageToken,
			Query:         config.adjustedQuery,
		})
		if err != nil {
			var invalidArgErr *serviceerror.InvalidArgument
			if errors.As(err, &invalidArgErr) {
				return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", err)
			}
			return nil, err
		}

		executionInfos = make([]*workflowpb.WorkflowExecutionInfo, 0, len(resp.Executions))
		executionInfos = append(executionInfos, resp.Executions...)
		nextPageToken = resp.NextPageToken
	}

	return &page{
		executionInfos:      executionInfos,
		targetExecutionInfo: targetExecutionInfo,
		nextPageToken:       nextPageToken,
		pageNumber:          pageNumber,
	}, nil
}

// processWorkflowsWithProactiveFetching handles the core logic for both batch activity functions
// nolint:revive,cognitive-complexity
func (a *activities) processWorkflowsWithProactiveFetching(
	ctx context.Context,
	config batchProcessorConfig,
	startWorkerProcessor batchWorkerProcessor,
	rateLimiter quotas.RequestRateLimiter,
	sdkClient sdkclient.Client,
	metricsHandler metrics.Handler,
	logger log.Logger,
	hbd HeartBeatDetails,
) (HeartBeatDetails, error) {

	concurrency := int(math.Max(1, float64(config.concurrency)))

	taskCh := make(chan task, concurrency)
	respCh := make(chan taskResponse, concurrency)

	// Ticker for frequent heartbeats to avoid timeout during slow processing, 1/4 of the default heartbeat timeout (10s)
	heartbeatTicker := time.NewTicker(defaultActivityHeartBeatTimeout / 4)
	defer heartbeatTicker.Stop()

	for range concurrency {
		go startWorkerProcessor(ctx, taskCh, respCh, rateLimiter, sdkClient, a.FrontendClient, metricsHandler, logger)
	}

	// Initialize the first p from initial executions or fetch from query
	var p *page
	if len(config.initialTargetExecutions) > 0 {
		if isActivityBatchType(config.batchType) {
			p = &page{
				targetExecutionInfo: config.initialTargetExecutions,
				nextPageToken:       config.initialPageToken,
				pageNumber:          hbd.CurrentPage,
			}
		} else {
			// Workflow batch types process tasks via executionInfo, not
			// targetExecutionInfo; convert the target executions
			// (business_id/run_id) into WorkflowExecutionInfo.
			executionInfos := make([]*workflowpb.WorkflowExecutionInfo, 0, len(config.initialTargetExecutions))
			for _, exec := range config.initialTargetExecutions {
				executionInfos = append(executionInfos, &workflowpb.WorkflowExecutionInfo{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: exec.GetBusinessId(),
						RunId:      exec.GetRunId(),
					},
				})
			}
			p = &page{
				executionInfos: executionInfos,
				nextPageToken:  config.initialPageToken,
				pageNumber:     hbd.CurrentPage,
			}
		}
	} else if len(config.initialExecutions) > 0 {
		if isActivityBatchType(config.batchType) {
			targetExecutions := make([]*commonpb.Execution, 0, len(config.initialExecutions))
			for _, exec := range config.initialExecutions {
				targetExecutions = append(targetExecutions, &commonpb.Execution{
					Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
					BusinessId: exec.GetWorkflowId(),
					RunId:      exec.GetRunId(),
				})
			}
			p = &page{
				targetExecutionInfo: targetExecutions,
				nextPageToken:       config.initialPageToken,
				pageNumber:          hbd.CurrentPage,
			}
		} else {
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
		}

		// Disable this send case (nil channel) once the page is fully submitted, so the loop
		// waits on results instead of offering empty tasks.
		var submitCh chan task
		var nextTask task
		if p.submittedCount < p.length() {
			submitCh = taskCh
			nextTask = p.nextTask()
		}

		select {
		case submitCh <- nextTask:
			p.submittedCount++

		case result := <-respCh:
			resultPage := result.page
			if result.err == nil {
				resultPage.successCount++
			} else {
				resultPage.errorCount++
			}

			recordCompletedPages(&hbd, resultPage)

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

// checkNamespace validates that batchParams targets the worker's own namespace.
// The NamespaceId, Request.Namespace (if set), and AdminRequest.Namespace (if set)
// must all agree with the worker's bound namespace. This prevents cross-namespace
// escalation via the privileged internal-frontend connection (NoopClaimMapper → RoleAdmin).
func (a *activities) checkNamespace(batchParams *batchspb.BatchOperationInput) error {
	if batchParams.NamespaceId != a.namespaceID.String() {
		return errNamespaceMismatch
	}
	ns := a.namespace.String()
	if req := batchParams.GetRequest(); req != nil && req.GetNamespace() != ns {
		return errNamespaceMismatch
	}
	if req := batchParams.GetAdminRequest(); req != nil && req.GetNamespace() != ns {
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

	if err := a.checkNamespace(batchParams); err != nil {
		metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
		logger.Error("Failed to run batch operation due to namespace mismatch", tag.Error(err))
		return hbd, err
	}
	ns := a.namespace.String()

	sdkClient := a.ClientFactory.NewClient(sdkclient.Options{
		Namespace:     ns,
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

	// Get executions based on request type (public vs admin).
	var visibilityQuery string
	var executions []*commonpb.WorkflowExecution
	var targetExecutions []*commonpb.Execution

	// Admin batch uses the host level rate limiter which applies across all namespaces and all admin batch workflows.
	rateLimiter := quotas.RequestRateLimiter(a.AdminBatcherRateLimiter)

	if batchParams.AdminRequest != nil {
		ctx = headers.SetCallerType(ctx, headers.CallerTypePreemptable)
		adminReq := batchParams.AdminRequest
		visibilityQuery = adminReq.GetVisibilityQuery()
		executions = adminReq.GetExecutions()
	} else {
		visibilityQuery = a.adjustQueryBatchTypeEnum(batchParams.Request.VisibilityQuery, batchParams.BatchType)
		//nolint:staticcheck // SA1019: Executions is deprecated but still needed for backward compatibility
		executions = batchParams.Request.Executions
		targetExecutions = batchParams.Request.GetTargetExecutions()
		rateLimiter = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 {
			return float64(a.rps(ns))
		}))
	}

	if startOver {
		estimateCount := int64(len(executions) + len(targetExecutions)) // NOTE: only one of these will ever be > 0
		if len(visibilityQuery) > 0 {
			var count int64
			var err error
			if isActivityBatchType(batchParams.BatchType) {
				// Activity batch types operate on activity executions, which are
				// counted via CountActivityExecutions rather than CountWorkflow.
				var resp *workflowservice.CountActivityExecutionsResponse
				resp, err = sdkClient.WorkflowService().CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
					Namespace: ns,
					Query:     visibilityQuery,
				})
				if err == nil {
					count = resp.GetCount()
				}
			} else {
				var resp *workflowservice.CountWorkflowExecutionsResponse
				resp, err = sdkClient.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
					Query: visibilityQuery,
				})
				if err == nil {
					count = resp.GetCount()
				}
			}
			if err != nil {
				metrics.BatcherOperationFailures.With(metricsHandler).Record(1)
				logger.Error("Failed to get estimate execution count", tag.Error(err))
				var invalidArgErr *serviceerror.InvalidArgument
				if errors.As(err, &invalidArgErr) {
					return HeartBeatDetails{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", err)
				}
				return HeartBeatDetails{}, err
			}
			estimateCount = count
		}
		hbd.TotalEstimate = estimateCount
	}

	// Prepare configuration for shared processing function
	config := batchProcessorConfig{
		namespace:               ns,
		adjustedQuery:           visibilityQuery,
		batchType:               batchParams.BatchType,
		concurrency:             a.getOperationConcurrency(int(batchParams.Concurrency)),
		initialPageToken:        hbd.PageToken,
		initialExecutions:       executions,
		initialTargetExecutions: targetExecutions,
	}

	// Create a wrapper for the task processor
	workerProcessor := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		rateLimiter quotas.RequestRateLimiter,
		sdkClient sdkclient.Client,
		frontendClient workflowservice.WorkflowServiceClient,
		metricsHandler metrics.Handler,
		logger log.Logger,
	) {
		a.startTaskProcessor(ctx, batchParams, ns, taskCh, respCh, rateLimiter, sdkClient, frontendClient, metricsHandler, logger)
	}

	return a.processWorkflowsWithProactiveFetching(ctx, config, workerProcessor, rateLimiter, sdkClient, metricsHandler, logger, hbd)
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

	//nolint:staticcheck // SA1019: in-flight batches may have legacy enum values
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE, enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		enumspb.BATCH_OPERATION_TYPE_SIGNAL, enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
		enumspb.BATCH_OPERATION_TYPE_CANCEL, enumspb.BATCH_OPERATION_TYPE_CANCEL_WORKFLOW,
		enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS, enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
		enumspb.BATCH_OPERATION_TYPE_UNPAUSE_ACTIVITY, enumspb.BATCH_OPERATION_TYPE_UPDATE_ACTIVITY_OPTIONS, enumspb.BATCH_OPERATION_TYPE_RESET_ACTIVITY,
		enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY:
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

// taskTimeoutContext derives a context bounded by defaultTaskTimeout for
// processing a single task. If the parent context already has a deadline that
// is sooner than defaultTaskTimeout, the parent context is returned unchanged
// (with a no-op cancel) so we never extend an existing, shorter deadline.
func taskTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= defaultTaskTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultTaskTimeout)
}

func (a *activities) startTaskProcessor(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	namespace string,
	taskCh chan task,
	respCh chan taskResponse,
	limiter quotas.RequestRateLimiter,
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

			if task.executionInfo == nil && task.targetExecution == nil {
				continue
			}

			a.processTaskWithRetries(ctx, batchOperation, namespace, task, respCh, limiter, sdkClient, frontendClient, metricsHandler, logger)
		}
	}
}

// deterministicRequestID derives a stable request ID from the batch job and
// the identifying fields of the target of a single call. Deriving the request ID
// lets the server's idempotency checks collapse retries of the same call.
func deterministicRequestID(jobID string, parts ...string) string {
	key := jobID
	for _, part := range parts {
		key += ":" + part
	}
	return uuid.NewSHA1(uuid.Nil, []byte(key)).String()
}

// processSingleTask processes a single batch task, bounding its execution with a
// per-task timeout so that one hung operation cannot block the task processor.
// nolint:revive,cognitive-complexity
func (a *activities) processSingleTask(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	namespace string,
	task task,
	limiter quotas.RequestRateLimiter,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	logger log.Logger,
) error {
	var err error

	// Bound the processing of each individual task so a single hung operation
	// cannot block the processor indefinitely.
	ctx, cancel := taskTimeoutContext(ctx)
	defer cancel()

	// Handle admin batch operations
	if batchOperation.AdminRequest != nil {
		return a.processAdminTask(ctx, batchOperation, task, limiter)
	}

	switch operation := batchOperation.Request.Operation.(type) {
	case *workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation:
		err = processTargetTask(ctx, limiter, task,
			func(execution *commonpb.Execution) error {
				_, err := frontendClient.TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
					Namespace:  namespace,
					ActivityId: execution.GetBusinessId(),
					RunId:      execution.GetRunId(),
					Identity:   operation.TerminateActivitiesOperation.GetIdentity(),
					Reason:     operation.TerminateActivitiesOperation.GetReason(),
					RequestId:  deterministicRequestID(batchOperation.Request.GetJobId(), "terminate-activity", execution.GetBusinessId(), execution.GetRunId()),
				})
				return err
			})
	case *workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation:
		err = processTargetTask(ctx, limiter, task,
			func(execution *commonpb.Execution) error {
				_, err := frontendClient.DeleteActivityExecution(ctx, &workflowservice.DeleteActivityExecutionRequest{
					Namespace:  namespace,
					ActivityId: execution.GetBusinessId(),
					RunId:      execution.GetRunId(),
				})
				return err
			})
	case *workflowservice.StartBatchOperationRequest_CancelActivitiesOperation:
		err = processTargetTask(ctx, limiter, task,
			func(execution *commonpb.Execution) error {
				_, err := frontendClient.RequestCancelActivityExecution(ctx, &workflowservice.RequestCancelActivityExecutionRequest{
					Namespace:  namespace,
					ActivityId: execution.GetBusinessId(),
					RunId:      execution.GetRunId(),
					Identity:   operation.CancelActivitiesOperation.GetIdentity(),
					Reason:     operation.CancelActivitiesOperation.GetReason(),
					RequestId:  deterministicRequestID(batchOperation.Request.GetJobId(), "cancel-activity", execution.GetBusinessId(), execution.GetRunId()),
				})
				return err
			})
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
				_, err := frontendClient.SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         namespace,
					WorkflowExecution: executionInfo.Execution,
					SignalName:        operation.SignalOperation.GetSignal(),
					Input:             operation.SignalOperation.GetInput(),
					Identity:          operation.SignalOperation.GetIdentity(),
					RequestId: deterministicRequestID(batchOperation.Request.GetJobId(), "signal",
						executionInfo.Execution.GetWorkflowId(), executionInfo.Execution.GetRunId(), operation.SignalOperation.GetSignal()),
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
				var eventID int64
				var err error
				//nolint:staticcheck // SA1019: worker versioning v0.31
				var resetReapplyType enumspb.ResetReapplyType
				var resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType
				if operation.ResetOperation.Options != nil {
					// Using ResetOptions
					// Note: getResetEventIDByOptions may modify workflowExecution.RunId, if reset should be to a prior run
					//nolint:staticcheck // SA1019: worker versioning v0.31
					eventID, err = getResetEventIDByOptions(ctx, operation.ResetOperation.Options, namespace, executionInfo.Execution, frontendClient, logger)
					//nolint:staticcheck // SA1019: worker versioning v0.31
					resetReapplyType = operation.ResetOperation.Options.ResetReapplyType
					//nolint:staticcheck // SA1019: worker versioning v0.31
					resetReapplyExcludeTypes = operation.ResetOperation.Options.ResetReapplyExcludeTypes
				} else {
					// Old fields
					//nolint:staticcheck // SA1019: worker versioning v0.31
					eventID, err = getResetEventIDByType(ctx, operation.ResetOperation.ResetType, namespace, executionInfo.Execution, frontendClient, logger)
					//nolint:staticcheck // SA1019: worker versioning v0.31
					resetReapplyType = operation.ResetOperation.ResetReapplyType
				}
				if err != nil {
					return err
				}
				_, err = frontendClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
					Namespace:         namespace,
					WorkflowExecution: executionInfo.Execution,
					Reason:            batchOperation.Request.Reason,
					RequestId: deterministicRequestID(batchOperation.Request.GetJobId(), "reset",
						executionInfo.Execution.GetWorkflowId(), executionInfo.Execution.GetRunId()),
					WorkflowTaskFinishEventId: eventID,
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
					return fmt.Errorf("unknown activity type: %v", operation.UnpauseActivitiesOperation.GetActivity())
				}

				_, err := frontendClient.UnpauseActivity(ctx, unpauseRequest)
				return err
			})

	case *workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation:
		err = processTask(ctx, limiter, task,
			func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
				_, err := frontendClient.UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
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
					return fmt.Errorf("unknown activity type: %v", operation.ResetActivitiesOperation.GetActivity())
				}

				_, err := frontendClient.ResetActivity(ctx, resetRequest)
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
					return fmt.Errorf("unknown activity type: %v", operation.UpdateActivityOptionsOperation.GetActivity())
				}

				updateRequest.ActivityOptions = operation.UpdateActivityOptionsOperation.GetActivityOptions()
				_, err := frontendClient.UpdateActivityOptions(ctx, updateRequest)
				return err
			})
	default:
		err = fmt.Errorf("unknown batch type: %v", batchOperation.BatchType)
	}
	return err
}

// isNonRetryableError determines if an error should not be retried based on the operation type
func isNonRetryableError(err error, batchType enumspb.BatchOperationType) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Operation-specific non-retryable errors
	//nolint:staticcheck // SA1019: in-flight batches may have the legacy enum value
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS, enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS:
		// Pinned version that is not present in a task queue error is non-retryable for workflow options updates
		return strings.Contains(errMsg, worker_versioning.ErrPinnedVersionNotInTaskQueueSubstring)
	default:
		return false
	}
}

// processTaskWithRetries runs the task's operation, retrying retryable failures in place on
// this goroutine, and sends exactly one response on respCh per task.
func (a *activities) processTaskWithRetries(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	ns string,
	task task,
	respCh chan taskResponse,
	limiter quotas.RequestRateLimiter,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
) {
	var err error
	for {
		err = a.processSingleTask(ctx, batchOperation, ns, task, limiter, sdkClient, frontendClient, logger)
		if err == nil {
			metrics.BatcherProcessorSuccess.With(metricsHandler).Record(1)
			break
		}

		metrics.BatcherProcessorFailures.With(metricsHandler).Record(1)
		logger.Error("Failed to process batch operation task", tag.Error(err))

		if isRetryableTaskError(err, batchOperation) && task.attempts <= int(batchOperation.AttemptsOnRetryableError) && !isDone(ctx) {
			task.attempts++
			continue
		}
		break
	}

	// Send one response per task; stop early if the context is cancelled.
	select {
	case respCh <- taskResponse{err: err, page: task.page}:
	case <-ctx.Done():
	}
}

// isRetryableTaskError reports whether a failed task's error permits a retry.
func isRetryableTaskError(err error, batchOperation *batchspb.BatchOperationInput) bool {
	var appErr *temporal.ApplicationError
	nonRetryable := (errors.As(err, &appErr) && appErr.NonRetryable()) ||
		isNonRetryableError(err, batchOperation.BatchType) ||
		slices.Contains(batchOperation.NonRetryableErrors, err.Error())
	return !nonRetryable
}

func (a *activities) processAdminTask(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	task task,
	limiter quotas.RequestRateLimiter,
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
	limiter quotas.RequestRateLimiter,
	task task,
	procFn func(*workflowpb.WorkflowExecutionInfo) error,
) error {
	err := limiter.Wait(ctx, batchQuotaRequest)
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

// processTargetTask is the activity-execution counterpart of processTask,
// used by the terminate/cancel/delete activity batch operations whose tasks
// carry an activity target execution rather than a workflow execution.
func processTargetTask(
	ctx context.Context,
	limiter quotas.RequestRateLimiter,
	task task,
	procFn func(*commonpb.Execution) error,
) error {
	err := limiter.Wait(ctx, batchQuotaRequest)
	if err != nil {
		return err
	}

	err = procFn(task.targetExecution)
	if err != nil {
		// NotFound means the activity is not running or already deleted
		if !common.IsNotFoundError(err) {
			return err
		}
	}

	return nil
}

// isActivityBatchType reports whether the batch operation type operates on
// activity executions (listed via ListActivityExecutions) rather than workflow
// executions.
func isActivityBatchType(batchType enumspb.BatchOperationType) bool {
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
		enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
		enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY:
		return true
	default:
		return false
	}
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
