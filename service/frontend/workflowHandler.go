// Copyright (c) 2019 Temporal Technologies, Inc.
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

package frontend

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/elasticsearch/validator"
	"github.com/temporalio/temporal/common/headers"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/resource"
)

type (
	// WorkflowHandler - gRPC handler interface for workflowservice
	WorkflowHandler struct {
		resource.Resource

		tokenSerializer           common.TaskTokenSerializer
		rateLimiter               quotas.Policy
		config                    *Config
		versionChecker            headers.VersionChecker
		domainHandler             domain.Handler
		visibilityQueryValidator  *validator.VisibilityQueryValidator
		searchAttributesValidator *validator.SearchAttributesValidator
	}
)

var (
	_                          workflowservice.WorkflowServiceServer = (*WorkflowHandler)(nil)
	frontendServiceRetryPolicy                                       = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a gRPC handler for workflowservice
func NewWorkflowHandler(
	resource resource.Resource,
	config *Config,
	replicationMessageSink messaging.Producer,
) *WorkflowHandler {
	handler := &WorkflowHandler{
		Resource:        resource,
		config:          config,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
		rateLimiter: quotas.NewMultiStageRateLimiter(
			func() float64 {
				return float64(config.RPS())
			},
			func(domain string) float64 {
				return float64(config.DomainRPS(domain))
			},
		),
		versionChecker: headers.NewVersionChecker(),
		domainHandler: domain.NewHandler(
			config.MinRetentionDays(),
			config.MaxBadBinaries,
			resource.GetLogger(),
			resource.GetMetadataManager(),
			resource.GetClusterMetadata(),
			domain.NewDomainReplicator(replicationMessageSink, resource.GetLogger()),
			resource.GetArchivalMetadata(),
			resource.GetArchiverProvider(),
		),
		visibilityQueryValidator: validator.NewQueryValidator(config.ValidSearchAttributes),
		searchAttributesValidator: validator.NewSearchAttributesValidator(
			resource.GetLogger(),
			config.ValidSearchAttributes,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
	}

	return handler
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, request *workflowservice.RegisterDomainRequest) (_ *workflowservice.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendRegisterDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetWorkflowExecutionRetentionPeriodInDays() > common.MaxWorkflowRetentionPeriodInDays {
		return nil, errInvalidRetention
	}

	if err := wh.checkPermission(wh.config, request.SecurityToken); err != nil {
		return nil, err
	}

	if request.GetName() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.RegisterDomain(ctx, request)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return resp, nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(ctx context.Context, request *workflowservice.DescribeDomainRequest) (_ *workflowservice.DescribeDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendDescribeDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetName() == "" && request.GetUuid() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.DescribeDomain(ctx, request)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// ListDomains returns the information and configuration for all domains.
func (wh *WorkflowHandler) ListDomains(ctx context.Context, request *workflowservice.ListDomainsRequest) (_ *workflowservice.ListDomainsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendListDomainsScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.domainHandler.ListDomains(ctx, request)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(ctx context.Context, request *workflowservice.UpdateDomainRequest) (_ *workflowservice.UpdateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendUpdateDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	// don't require permission for failover request
	if !wh.isFailoverRequest(request) {
		if err := wh.checkPermission(wh.config, request.SecurityToken); err != nil {
			return nil, err
		}
	}

	if request.GetName() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.UpdateDomain(ctx, request)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, request *workflowservice.DeprecateDomainRequest) (_ *workflowservice.DeprecateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendDeprecateDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.checkPermission(wh.config, request.SecurityToken); err != nil {
		return nil, err
	}

	if request.GetName() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.DeprecateDomain(ctx, request)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, err
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
// first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowHandler) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendStartWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	domainName := request.GetDomain()
	if domainName == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(domainName) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if request.GetWorkflowId() == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}

	if len(request.GetWorkflowId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowIDTooLong, scope)
	}

	if err := common.ValidateRetryPolicy(request.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.GetLogger().Debug(
		"Received StartWorkflowExecution. WorkflowID",
		tag.WorkflowID(request.GetWorkflowId()))

	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return nil, wh.error(errWorkflowTypeNotSet, scope)
	}

	if len(request.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowTypeTooLong, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	if request.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidExecutionStartToCloseTimeoutSeconds, scope)
	}

	if request.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidTaskStartToCloseTimeoutSeconds, scope)
	}

	if request.GetRequestId() == "" {
		return nil, wh.error(errRequestIDNotSet, scope)
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errRequestIDTooLong, scope)
	}

	if err := wh.searchAttributesValidator.ValidateSearchAttributes(request.SearchAttributes, domainName); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.GetLogger().Debug("Start workflow execution request domain", tag.WorkflowDomainName(domainName))
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainName))

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	actualSize := len(request.Input)
	if request.Memo != nil {
		actualSize += common.GetSizeOfMapStringToByteArray(request.Memo.GetFields())
	}
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		request.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.GetLogger().Debug("Start workflow execution request domainID", tag.WorkflowDomainID(domainID))
	resp, err := wh.GetHistoryClient().StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(domainID, request))

	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: resp.GetRunId()}, nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
// execution in unknown to the service.
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendGetWorkflowExecutionHistoryScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(request.GetDomain()))
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// force limit page size if exceed
	if request.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetThrottledLogger().Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowDomainID(domainID), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	enableArchivalRead := wh.GetArchivalMetadata().GetHistoryConfig().ReadEnabled()
	historyArchived := wh.historyArchived(ctx, request, domainID)
	if enableArchivalRead && historyArchived {
		return wh.getArchivedHistory(ctx, request, domainID, scope)
	}

	// this function return the following 5 things,
	// 1. the workflow run ID
	// 2. the last first event ID (the event ID of the last batch of events in the history)
	// 3. the next event ID
	// 4. whether the workflow is closed
	// 5. error if any
	queryHistory := func(
		domainUUID string,
		execution *commonproto.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, int64, bool, error) {
		response, err := wh.GetHistoryClient().PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			DomainUUID:          domainUUID,
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowCloseState() == persistence.WorkflowCloseStatusRunning

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			nil
	}

	isLongPoll := request.GetWaitForNewEvent()
	isCloseEventOnly := request.GetHistoryEventFilterType() == enums.HistoryEventFilterTypeCloseEvent
	execution := request.Execution
	var continuationToken *token.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if request.NextPageToken != nil {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = continuationToken.GetRunId()

		// we need to update the current next event ID and whether workflow is running
		if len(continuationToken.PersistenceToken) == 0 && isLongPoll && continuationToken.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = continuationToken.GetNextEventId()
			}
			continuationToken.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err =
				queryHistory(domainID, execution, queryNextEventID, continuationToken.BranchToken)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			continuationToken.FirstEventId = continuationToken.GetNextEventId()
			continuationToken.NextEventId = nextEventID
			continuationToken.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		continuationToken = &token.HistoryContinuation{}
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		continuationToken.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err =
			queryHistory(domainID, execution, queryNextEventID, nil)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		execution.RunId = runID

		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = nextEventID
		continuationToken.IsWorkflowRunning = isWorkflowRunning
		continuationToken.PersistenceToken = nil
	}

	history := &commonproto.History{}
	history.Events = []*commonproto.HistoryEvent{}
	if isCloseEventOnly {
		if !isWorkflowRunning {
			history, _, err = wh.getHistory(
				scope,
				domainID,
				*execution,
				lastFirstEventID,
				nextEventID,
				request.GetMaximumPageSize(),
				nil,
				continuationToken.TransientDecision,
				continuationToken.BranchToken,
			)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			// since getHistory func will not return empty history, so the below is safe
			history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			continuationToken = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			continuationToken.PersistenceToken = nil
		} else {
			continuationToken = nil
		}
	} else {
		// return all events
		if continuationToken.FirstEventId >= continuationToken.NextEventId {
			// currently there is no new event
			history.Events = []*commonproto.HistoryEvent{}
			if !isWorkflowRunning {
				continuationToken = nil
			}
		} else {
			history, continuationToken.PersistenceToken, err = wh.getHistory(
				scope,
				domainID,
				*execution,
				continuationToken.FirstEventId,
				continuationToken.NextEventId,
				request.GetMaximumPageSize(),
				continuationToken.PersistenceToken,
				continuationToken.TransientDecision,
				continuationToken.BranchToken,
			)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(continuationToken.PersistenceToken) == 0 && (!continuationToken.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				continuationToken = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: nextToken,
		Archived:      false,
	}, nil
}

// PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
// Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
// It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
// application worker.
func (wh *WorkflowHandler) PollForDecisionTask(ctx context.Context, request *workflowservice.PollForDecisionTaskRequest) (_ *workflowservice.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	tagsForErrorLog := []tag.Tag{tag.WorkflowDomainName(request.GetDomain())}
	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForDecisionTaskScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope, tagsForErrorLog...)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope, tagsForErrorLog...)
	}

	wh.GetLogger().Debug("Received PollForDecisionTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForDecisionTask",
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope, tagsForErrorLog...)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope, tagsForErrorLog...)
	}
	if len(request.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope, tagsForErrorLog...)
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope, tagsForErrorLog...)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	domainName := request.GetDomain()
	domainEntry, err := wh.GetDomainCache().GetDomain(domainName)
	if err != nil {
		return nil, wh.error(err, scope, tagsForErrorLog...)
	}
	domainID := domainEntry.GetInfo().ID

	wh.GetLogger().Debug("Poll for decision.", tag.WorkflowDomainName(domainName), tag.WorkflowDomainID(domainID))
	if err := wh.checkBadBinary(domainEntry, request.GetBinaryChecksum()); err != nil {
		return nil, wh.error(err, scope, tagsForErrorLog...)
	}

	pollerID := uuid.New()
	var matchingResp *matchingservice.PollForDecisionTaskResponse
	op := func() error {
		var err error
		matchingResp, err = wh.GetMatchingClient().PollForDecisionTask(ctx, &matchingservice.PollForDecisionTaskRequest{
			DomainUUID:  domainID,
			PollerID:    pollerID,
			PollRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeDecision, request.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.GetLogger().Error("PollForDecisionTask failed.",
				tag.WorkflowTaskListName(request.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, wh.error(err, scope)
		}

		// Must be cancellation error.  Does'nt matter what we return here.  Client already went away.
		return nil, nil
	}

	tagsForErrorLog = append(tagsForErrorLog, []tag.Tag{tag.WorkflowID(
		matchingResp.GetWorkflowExecution().GetWorkflowId()),
		tag.WorkflowRunID(matchingResp.GetWorkflowExecution().GetRunId())}...)
	resp, err := wh.createPollForDecisionTaskResponse(ctx, scope, domainID, matchingResp, matchingResp.GetBranchToken())
	if err != nil {
		return nil, wh.error(err, scope, tagsForErrorLog...)
	}
	return resp, nil
}

// RespondDecisionTaskCompleted is called by application worker to complete a DecisionTask handed as a result of
// 'PollForDecisionTask' API call.  Completing a DecisionTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding decisions.  It will also create a DecisionTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollForDecisionTask API call
// for completing the DecisionTask.
// The response could contain a new decision task if there is one or if the request asking for one.
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(ctx context.Context, request *workflowservice.RespondDecisionTaskCompletedRequest) (_ *workflowservice.RespondDecisionTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondDecisionTaskCompletedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainId := primitives.UUIDString(taskToken.GetDomainId())
	if domainId == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainId)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondDecisionTaskCompletedScope, domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	histResp, err := wh.GetHistoryClient().RespondDecisionTaskCompleted(ctx, &historyservice.RespondDecisionTaskCompletedRequest{
		DomainUUID:      domainId,
		CompleteRequest: request},
	)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	completedResp := &workflowservice.RespondDecisionTaskCompletedResponse{}
	if request.GetReturnNewDecisionTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := &token.Task{
			DomainId:        taskToken.GetDomainId(),
			WorkflowId:      taskToken.GetWorkflowId(),
			RunId:           taskToken.GetRunId(),
			ScheduleId:      histResp.StartedResponse.GetScheduledEventId(),
			ScheduleAttempt: histResp.StartedResponse.GetAttempt(),
		}
		token, _ := wh.tokenSerializer.Serialize(taskToken)
		workflowExecution := &commonproto.WorkflowExecution{
			WorkflowId: taskToken.GetWorkflowId(),
			RunId:      primitives.UUIDString(taskToken.GetDomainId()),
		}
		matchingResp := common.CreateMatchingPollForDecisionTaskResponse(histResp.StartedResponse, workflowExecution, token)

		newDecisionTask, err := wh.createPollForDecisionTaskResponse(ctx, scope, domainId, matchingResp, matchingResp.GetBranchToken())
		if err != nil {
			return nil, wh.error(err, scope)
		}
		completedResp.DecisionTask = newDecisionTask
	}

	return completedResp, nil
}

// RespondDecisionTaskFailed is called by application worker to indicate failure.  This results in
// DecisionTaskFailedEvent written to the history and a new DecisionTask created.  This API can be used by client to
// either clear sticky tasklist or report any panics during DecisionTask processing.  Cadence will only append first
// DecisionTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowHandler) RespondDecisionTaskFailed(ctx context.Context, request *workflowservice.RespondDecisionTaskFailedRequest) (_ *workflowservice.RespondDecisionTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondDecisionTaskFailedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	domainId := primitives.UUIDString(taskToken.GetDomainId())
	if domainId == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainId)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondDecisionTaskFailedScope, domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainId,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceed, we would just truncate the size for decision task failed as the details is not used anywhere by client code
		request.Details = request.Details[0:sizeLimitError]
	}

	_, err = wh.GetHistoryClient().RespondDecisionTaskFailed(ctx, &historyservice.RespondDecisionTaskFailedRequest{
		DomainUUID:    domainId,
		FailedRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.RespondDecisionTaskFailedResponse{}, nil
}

// PollForActivityTask is called by application worker to process ActivityTask from a specific taskList.  ActivityTask
// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowHandler) PollForActivityTask(ctx context.Context, request *workflowservice.PollForActivityTaskRequest) (_ *workflowservice.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForActivityTaskScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	wh.GetLogger().Debug("Received PollForActivityTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForActivityTask",
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(request.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	pollerID := uuid.New()
	var matchingResponse *matchingservice.PollForActivityTaskResponse
	op := func() error {
		var err error
		matchingResponse, err = wh.GetMatchingClient().PollForActivityTask(ctx, &matchingservice.PollForActivityTaskRequest{
			DomainUUID:  domainID,
			PollerID:    pollerID,
			PollRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeActivity, request.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.GetLogger().Error("PollForActivityTask failed.",
				tag.WorkflowTaskListName(request.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, wh.error(err, scope)
		}
	}

	if matchingResponse == nil {
		return nil, nil
	}

	return &workflowservice.PollForActivityTaskResponse{
		TaskToken:                       matchingResponse.TaskToken,
		WorkflowExecution:               matchingResponse.WorkflowExecution,
		ActivityId:                      matchingResponse.ActivityId,
		ActivityType:                    matchingResponse.ActivityType,
		Input:                           matchingResponse.Input,
		ScheduledTimestamp:              matchingResponse.ScheduledTimestamp,
		ScheduleToCloseTimeoutSeconds:   matchingResponse.ScheduleToCloseTimeoutSeconds,
		StartedTimestamp:                matchingResponse.StartedTimestamp,
		StartToCloseTimeoutSeconds:      matchingResponse.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:         matchingResponse.HeartbeatTimeoutSeconds,
		Attempt:                         matchingResponse.Attempt,
		ScheduledTimestampOfThisAttempt: matchingResponse.ScheduledTimestampOfThisAttempt,
		HeartbeatDetails:                matchingResponse.HeartbeatDetails,
		WorkflowType:                    matchingResponse.WorkflowType,
		WorkflowDomain:                  matchingResponse.WorkflowDomain,
		Header:                          matchingResponse.Header,
	}, nil
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for heartbeating.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRecordActivityTaskHeartbeatScope)

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	wh.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	domainId := primitives.UUIDString(taskToken.GetDomainId())
	if domainId == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainId)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRecordActivityTaskHeartbeatScope, domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainId,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Reason:    common.FailureReasonHeartbeatExceedsLimit,
			Details:   request.Details[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    domainId,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		return &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: true}, nil
	}

	resp, err := wh.GetHistoryClient().RecordActivityTaskHeartbeat(ctx, &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID:       domainId,
		HeartbeatRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: resp.GetCancelRequested()}, nil
}

// RecordActivityTaskHeartbeatByID is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatByID' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Domain, WorkflowID and ActivityID
func (wh *WorkflowHandler) RecordActivityTaskHeartbeatByID(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIDRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRecordActivityTaskHeartbeatByIDScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	wh.GetLogger().Debug("Received RecordActivityTaskHeartbeatByID")
	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := request.GetWorkflowID()
	runID := request.GetRunID() // runID is optional so can be empty
	activityID := request.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &token.Task{
		DomainId:   primitives.MustParseUUID(domainID),
		RunId:      primitives.MustParseUUID(runID),
		WorkflowId: workflowID,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.FailureReasonHeartbeatExceedsLimit,
			Details:   request.Details[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    domainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		return &workflowservice.RecordActivityTaskHeartbeatByIDResponse{CancelRequested: true}, nil
	}

	req := &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: token,
		Details:   request.Details,
		Identity:  request.Identity,
	}

	resp, err := wh.GetHistoryClient().RecordActivityTaskHeartbeat(ctx, &historyservice.RecordActivityTaskHeartbeatRequest{
		DomainUUID:       domainID,
		HeartbeatRequest: req,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.RecordActivityTaskHeartbeatByIDResponse{CancelRequested: resp.GetCancelRequested()}, nil
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCompleted(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskCompletedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	domainId := primitives.UUIDString(taskToken.GetDomainId())
	if domainId == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainId)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskCompletedScope,
		domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Result),
		sizeLimitWarn,
		sizeLimitError,
		domainId,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Reason:    common.FailureReasonCompleteResultExceedsLimit,
			Details:   request.Result[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    domainId,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	} else {
		_, err = wh.GetHistoryClient().RespondActivityTaskCompleted(ctx, &historyservice.RespondActivityTaskCompletedRequest{
			DomainUUID:      domainId,
			CompleteRequest: request,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskCompletedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Similar to RespondActivityTaskCompleted but use Domain,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCompletedByID(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIDRequest) (_ *workflowservice.RespondActivityTaskCompletedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskCompletedByIDScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := request.GetWorkflowID()
	runID := request.GetRunID() // runID is optional so can be empty
	activityID := request.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	taskToken := &token.Task{
		DomainId:   primitives.MustParseUUID(domainID),
		RunId:      primitives.MustParseUUID(runID),
		WorkflowId: workflowID,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Result),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		runID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.FailureReasonCompleteResultExceedsLimit,
			Details:   request.Result[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    domainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	} else {
		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: token,
			Result:    request.Result,
			Identity:  request.Identity,
		}

		_, err = wh.GetHistoryClient().RespondActivityTaskCompleted(ctx, &historyservice.RespondActivityTaskCompletedRequest{
			DomainUUID:      domainID,
			CompleteRequest: req,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return &workflowservice.RespondActivityTaskCompletedByIDResponse{}, nil
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskFailed(ctx context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskFailedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	domainID := primitives.UUIDString(taskToken.GetDomainId())
	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskFailedScope,
		domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		request.Reason = common.FailureReasonFailureDetailsExceedsLimit
		request.Details = request.Details[0:sizeLimitError]
	}

	_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID:    domainID,
		FailedRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskFailedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskFailed but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskFailedByID(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIDRequest) (_ *workflowservice.RespondActivityTaskFailedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskFailedByIDScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := request.GetWorkflowID()
	runID := request.GetRunID() // runID is optional so can be empty
	activityID := request.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	taskToken := &token.Task{
		DomainId:   primitives.MustParseUUID(domainID),
		RunId:      primitives.MustParseUUID(runID),
		WorkflowId: workflowID,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		runID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		request.Reason = common.FailureReasonFailureDetailsExceedsLimit
		request.Details = request.Details[0:sizeLimitError]
	}

	req := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: token,
		Reason:    request.Reason,
		Details:   request.Details,
		Identity:  request.Identity,
	}

	_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
		DomainUUID:    domainID,
		FailedRequest: req,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.RespondActivityTaskFailedByIDResponse{}, nil
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskCanceledScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainID := primitives.UUIDString(taskToken.GetDomainId())

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskCanceledScope,
		domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		primitives.UUIDString(taskToken.GetRunId()),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Reason:    common.FailureReasonCancelDetailsExceedsLimit,
			Details:   request.Details[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    primitives.UUIDString(taskToken.GetDomainId()),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	} else {
		_, err = wh.GetHistoryClient().RespondActivityTaskCanceled(ctx, &historyservice.RespondActivityTaskCanceledRequest{
			DomainUUID:    primitives.UUIDString(taskToken.GetDomainId()),
			CancelRequest: request,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return &workflowservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondActivityTaskCanceledByID is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskCanceled but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCanceledByID(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIDRequest) (_ *workflowservice.RespondActivityTaskCanceledByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskCanceledScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := request.GetWorkflowID()
	runID := request.GetRunID() // runID is optional so can be empty
	activityID := request.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	taskToken := &token.Task{
		DomainId:   primitives.MustParseUUID(domainID),
		RunId:      primitives.MustParseUUID(runID),
		WorkflowId: workflowID,
		ScheduleId: common.EmptyEventID,
		ActivityId: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.Details),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		taskToken.GetWorkflowId(),
		runID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.FailureReasonCancelDetailsExceedsLimit,
			Details:   request.Details[0:sizeLimitError],
			Identity:  request.Identity,
		}
		_, err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			DomainUUID:    domainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	} else {
		req := &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: token,
			Details:   request.Details,
			Identity:  request.Identity,
		}

		_, err = wh.GetHistoryClient().RespondActivityTaskCanceled(ctx, &historyservice.RespondActivityTaskCanceledRequest{
			DomainUUID:    domainID,
			CancelRequest: req,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return &workflowservice.RespondActivityTaskCanceledByIDResponse{}, nil
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRequestCancelWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.WorkflowExecution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.GetHistoryClient().RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
		DomainUUID:    domainID,
		CancelRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendSignalWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(request.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.WorkflowExecution, scope); err != nil {
		return nil, err
	}

	if request.GetSignalName() == "" {
		return nil, wh.error(errSignalNameTooLong, scope)
	}

	if len(request.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errSignalNameTooLong, scope)
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errRequestIDTooLong, scope)
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetDomain())
	if err := common.CheckEventBlobSizeLimit(
		len(request.Input),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		request.GetWorkflowExecution().GetWorkflowId(),
		request.GetWorkflowExecution().GetRunId(),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.GetHistoryClient().SignalWorkflowExecution(ctx, &historyservice.SignalWorkflowExecutionRequest{
		DomainUUID:    domainID,
		SignalRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a decision task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandler) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendSignalWithStartWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	domainName := request.GetDomain()
	if domainName == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(domainName) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if request.GetWorkflowId() == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}

	if len(request.GetWorkflowId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowIDTooLong, scope)
	}

	if request.GetSignalName() == "" {
		return nil, wh.error(errSignalNameNotSet, scope)
	}

	if len(request.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errSignalNameTooLong, scope)
	}

	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return nil, wh.error(errWorkflowTypeNotSet, scope)
	}

	if len(request.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowTypeTooLong, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errRequestIDTooLong, scope)
	}

	if request.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidExecutionStartToCloseTimeoutSeconds, scope)
	}

	if request.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidTaskStartToCloseTimeoutSeconds, scope)
	}

	if err := common.ValidateRetryPolicy(request.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.searchAttributesValidator.ValidateSearchAttributes(request.SearchAttributes, domainName); err != nil {
		return nil, wh.error(err, scope)
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	if err := common.CheckEventBlobSizeLimit(
		len(request.SignalInput),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		request.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}
	actualSize := len(request.Input) + common.GetSizeOfMapStringToByteArray(request.Memo.GetFields())
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		request.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	var runId string
	op := func() error {
		var err error
		resp, err := wh.GetHistoryClient().SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
			DomainUUID:             domainID,
			SignalWithStartRequest: request,
		})
		runId = resp.GetRunId()
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: runId}, nil
}

// ResetWorkflowExecution reset an existing workflow execution to DecisionTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandler) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendResetWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.WorkflowExecution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp, err := wh.GetHistoryClient().ResetWorkflowExecution(ctx, &historyservice.ResetWorkflowExecutionRequest{
		DomainUUID:   domainID,
		ResetRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.ResetWorkflowExecutionResponse{RunId: resp.GetRunId()}, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendTerminateWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.WorkflowExecution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.GetHistoryClient().TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
		DomainUUID:       domainID,
		TerminateRequest: request,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific domain.
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListOpenWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if request.StartTimeFilter == nil {
		return nil, wh.error(errStartTimeFilterNotSet, scope)
	}

	if request.StartTimeFilter.GetEarliestTime() > request.StartTimeFilter.GetLatestTime() {
		return nil, wh.error(errEarliestTimeIsGreaterThanLatestTime, scope)
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(request.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetMaximumPageSize(), request.GetDomain()) {
		return nil, wh.error(errPageSizeTooBig.MessageArgs(wh.config.ESIndexMaxResultWindow()), scope)
	}

	domain := request.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(request.GetMaximumPageSize()),
		NextPageToken:     request.NextPageToken,
		EarliestStartTime: request.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   request.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.GetLogger().Info("List open workflow with filter",
			tag.WorkflowDomainName(request.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.GetLogger().Info("List open workflow with filter",
			tag.WorkflowDomainName(request.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.ListOpenWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific domain.
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListClosedWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if request.StartTimeFilter == nil {
		return nil, wh.error(errStartTimeFilterNotSet, scope)
	}

	if request.StartTimeFilter.GetEarliestTime() > request.StartTimeFilter.GetLatestTime() {
		return nil, wh.error(errEarliestTimeIsGreaterThanLatestTime, scope)
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(request.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetMaximumPageSize(), request.GetDomain()) {
		return nil, wh.error(errPageSizeTooBig.MessageArgs(wh.config.ESIndexMaxResultWindow()), scope)
	}

	domain := request.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(request.GetMaximumPageSize()),
		NextPageToken:     request.NextPageToken,
		EarliestStartTime: request.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   request.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(request.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(request.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else if request.GetStatusFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByStatus(&persistence.ListClosedWorkflowExecutionsByStatusRequest{
				ListWorkflowExecutionsRequest: baseReq,
				Status:                        request.GetStatusFilter().GetCloseStatus(),
			})
		}
		wh.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(request.GetDomain()), tag.WorkflowListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.ListClosedWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific domain.
func (wh *WorkflowHandler) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetPageSize(), request.GetDomain()) {
		return nil, wh.error(errPageSizeTooBig.MessageArgs(wh.config.ESIndexMaxResultWindow()), scope)
	}

	if err := wh.visibilityQueryValidator.ValidateListRequestForQuery(request); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := request.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.ListWorkflowExecutionsRequestV2{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}
	persistenceResp, err := wh.GetVisibilityManager().ListWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.ListWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific domain.
func (wh *WorkflowHandler) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListArchivedWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetDomain()))
	}

	maxPageSize := wh.config.VisibilityArchivalQueryMaxPageSize()
	if int(request.GetPageSize()) > maxPageSize {
		return nil, wh.error(errPageSizeTooBig.MessageArgs(maxPageSize), scope)
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival() {
		return nil, wh.error(errClusterIsNotConfiguredForVisibilityArchival, scope)
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ReadEnabled() {
		return nil, wh.error(errClusterIsNotConfiguredForReadingArchivalVisibility, scope)
	}

	entry, err := wh.GetDomainCache().GetDomain(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if entry.GetConfig().VisibilityArchivalStatus != enums.ArchivalStatusEnabled {
		return nil, wh.error(errDomainIsNotConfiguredForVisibilityArchival, scope)
	}

	URI, err := archiver.NewURI(entry.GetConfig().VisibilityArchivalURI)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	visibilityArchiver, err := wh.GetArchiverProvider().GetVisibilityArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	archiverRequest := &archiver.QueryVisibilityRequest{
		DomainID:      entry.GetInfo().ID,
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}

	archiverResponse, err := visibilityArchiver.Query(ctx, URI, archiverRequest)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// special handling of ExecutionTime for cron or retry
	for _, execution := range archiverResponse.Executions {
		if execution.GetExecutionTime() == 0 {
			execution.ExecutionTime = execution.GetStartTime().GetValue()
		}
	}

	return &workflowservice.ListArchivedWorkflowExecutionsResponse{
		Executions:    archiverResponse.Executions,
		NextPageToken: archiverResponse.NextPageToken,
	}, nil
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific domain without order.
func (wh *WorkflowHandler) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendScanWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetPageSize(), request.GetDomain()) {
		return nil, wh.error(errPageSizeTooBig.MessageArgs(wh.config.ESIndexMaxResultWindow()), scope)
	}

	if err := wh.visibilityQueryValidator.ValidateScanRequestForQuery(request); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := request.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.ListWorkflowExecutionsRequestV2{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}
	persistenceResp, err := wh.GetVisibilityManager().ScanWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &workflowservice.ScanWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}
	return resp, nil
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific domain.
func (wh *WorkflowHandler) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendCountWorkflowExecutionsScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.visibilityQueryValidator.ValidateCountRequestForQuery(request); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := request.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.CountWorkflowExecutionsRequest{
		DomainUUID: domainID,
		Domain:     domain,
		Query:      request.GetQuery(),
	}
	persistenceResp, err := wh.GetVisibilityManager().CountWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &workflowservice.CountWorkflowExecutionsResponse{
		Count: persistenceResp.Count,
	}
	return resp, nil
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandler) GetSearchAttributes(ctx context.Context, _ *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendGetSearchAttributesScope)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	keys := wh.config.ValidSearchAttributes()
	resp := &workflowservice.GetSearchAttributesResponse{
		Keys: wh.convertIndexedKeyToProto(keys),
	}
	return resp, nil
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a DecisionTask for query)
// as a result of 'PollForDecisionTask' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowHandler) RespondQueryTaskCompleted(ctx context.Context, request *workflowservice.RespondQueryTaskCompletedRequest) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondQueryTaskCompletedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow("")

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if queryTaskToken.GetDomainId() == "" || queryTaskToken.GetTaskList() == "" || queryTaskToken.GetTaskId() == "" {
		return nil, wh.error(errInvalidTaskToken, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(queryTaskToken.GetDomainId())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondQueryTaskCompletedScope,
		domainEntry.GetInfo().Name,
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.GetQueryResult()),
		sizeLimitWarn,
		sizeLimitError,
		queryTaskToken.GetDomainId(),
		"",
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		request = &workflowservice.RespondQueryTaskCompletedRequest{
			TaskToken:     request.TaskToken,
			CompletedType: enums.QueryTaskCompletedTypeFailed,
			QueryResult:   nil,
			ErrorMessage:  err.Error(),
		}
	}

	headers := headers.GetValues(ctx, headers.ClientImplHeaderName, headers.FeatureVersionHeaderName)
	request.WorkerVersionInfo = &commonproto.WorkerVersionInfo{
		Impl:           headers[0],
		FeatureVersion: headers[1],
	}
	matchingRequest := &matchingservice.RespondQueryTaskCompletedRequest{
		DomainUUID:       queryTaskToken.GetDomainId(),
		TaskList:         &commonproto.TaskList{Name: queryTaskToken.GetTaskList()},
		TaskID:           queryTaskToken.GetTaskId(),
		CompletedRequest: request,
	}

	_, err = wh.GetMatchingClient().RespondQueryTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.RespondQueryTaskCompletedResponse{}, nil
}

// ResetStickyTaskList resets the sticky tasklist related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (wh *WorkflowHandler) ResetStickyTaskList(ctx context.Context, request *workflowservice.ResetStickyTaskListRequest) (_ *workflowservice.ResetStickyTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendResetStickyTaskListScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.GetHistoryClient().ResetStickyTaskList(ctx, &historyservice.ResetStickyTaskListRequest{
		DomainUUID: domainID,
		Execution:  request.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(ctx context.Context, request *workflowservice.QueryWorkflowRequest) (_ *workflowservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendQueryWorkflowScope, request.GetDomain())
	defer sw.Stop()

	if wh.config.DisallowQuery(request.GetDomain()) {
		return nil, wh.error(errQueryDisallowedForDomain, scope)
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	if request.Query == nil {
		return nil, wh.error(errQueryNotSet, scope)
	}

	if request.Query.GetQueryType() == "" {
		return nil, wh.error(errQueryTypeNotSet, scope)
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetDomain())

	if err := common.CheckEventBlobSizeLimit(
		len(request.GetQuery().GetQueryArgs()),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		request.GetExecution().GetWorkflowId(),
		request.GetExecution().GetRunId(),
		scope,
		wh.GetThrottledLogger()); err != nil {
		return nil, wh.error(err, scope)
	}

	req := &historyservice.QueryWorkflowRequest{
		DomainUUID: domainID,
		Request:    request,
	}
	hResponse, err := wh.GetHistoryClient().QueryWorkflow(ctx, req)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return hResponse.GetResponse(), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendDescribeWorkflowExecutionScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	response, err := wh.GetHistoryClient().DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		DomainUUID: domainID,
		Request:    request,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.DescribeWorkflowExecutionResponse{
		ExecutionConfiguration: response.GetExecutionConfiguration(),
		WorkflowExecutionInfo:  response.GetWorkflowExecutionInfo(),
		PendingActivities:      response.GetPendingActivities(),
		PendingChildren:        response.GetPendingChildren(),
	}, nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowHandler) DescribeTaskList(ctx context.Context, request *workflowservice.DescribeTaskListRequest) (_ *workflowservice.DescribeTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendDescribeTaskListScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	var matchingResponse *matchingservice.DescribeTaskListResponse
	op := func() error {
		var err error
		matchingResponse, err = wh.GetMatchingClient().DescribeTaskList(ctx, &matchingservice.DescribeTaskListRequest{
			DomainUUID:  domainID,
			DescRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.DescribeTaskListResponse{
		Pollers:        matchingResponse.Pollers,
		TaskListStatus: matchingResponse.TaskListStatus,
	}, nil
}

func (wh *WorkflowHandler) PollForWorkflowExecutionRawHistory(ctx context.Context, request *workflowservice.PollForWorkflowExecutionRawHistoryRequest) (_ *workflowservice.PollForWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForWorkflowExecutionRawHistoryScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(request.GetDomain()))
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// force limit page size if exceed
	if request.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetThrottledLogger().Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowDomainID(domainID), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	// this function return the following 5 things,
	// 1. the workflow run ID
	// 2. the last first event ID (the event ID of the last batch of events in the history)
	// 3. the next event ID
	// 4. whether the workflow is closed
	// 5. error if any
	queryHistory := func(
		domainUUID string,
		execution *commonproto.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, int64, bool, error) {
		response, err := wh.GetHistoryClient().PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			DomainUUID:          domainUUID,
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowCloseState() == persistence.WorkflowCloseStatusRunning

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			nil
	}

	isCloseEventOnly := request.GetHistoryEventFilterType() == enums.HistoryEventFilterTypeCloseEvent
	execution := request.Execution
	token := &token.HistoryContinuation{}

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if request.NextPageToken != nil {
		token, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.GetRunId() != "" && execution.GetRunId() != token.RunId {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = token.RunId

		// we need to update the current next event ID and whether workflow is running
		if len(token.PersistenceToken) == 0 && token.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = token.NextEventId
			}
			token.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err =
				queryHistory(domainID, execution, queryNextEventID, token.BranchToken)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			token.FirstEventId = token.NextEventId
			token.NextEventId = nextEventID
			token.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		token.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err =
			queryHistory(domainID, execution, queryNextEventID, nil)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		execution.RunId = runID
		token.RunId = runID
		token.FirstEventId = common.FirstEventID
		token.NextEventId = nextEventID
		token.IsWorkflowRunning = isWorkflowRunning
		token.PersistenceToken = nil
	}

	history := []*commonproto.DataBlob{}
	if isCloseEventOnly {
		if !isWorkflowRunning {
			history, _, err = wh.getRawHistory(
				scope,
				domainID,
				*execution,
				lastFirstEventID,
				nextEventID,
				request.GetMaximumPageSize(),
				nil,
				token.TransientDecision,
				token.BranchToken,
			)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			// since getHistory func will not return empty history, so the below is safe
			history = history[len(history)-1 : len(history)]
			token = nil
		} else {
			// set the persistence token to be nil so next time we will query history for updates
			token.PersistenceToken = nil
		}
	} else {
		// return all events
		if token.FirstEventId >= token.NextEventId {
			// currently there is no new event
			if !isWorkflowRunning {
				token = nil
			}
		} else {
			history, token.PersistenceToken, err = wh.getRawHistory(
				scope,
				domainID,
				*execution,
				token.FirstEventId,
				token.NextEventId,
				request.GetMaximumPageSize(),
				token.PersistenceToken,
				token.TransientDecision,
				token.BranchToken,
			)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(token.PersistenceToken) == 0 && (!token.IsWorkflowRunning) {
				// meaning, there is no more history to be returned
				token = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(token)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.PollForWorkflowExecutionRawHistoryResponse{
		RawHistory:    history,
		NextPageToken: nextToken,
	}, nil
}

// GetWorkflowExecutionRawHistory retrieves raw history directly from DB layer.
func (wh *WorkflowHandler) GetWorkflowExecutionRawHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionRawHistoryRequest) (_ *workflowservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendGetWorkflowExecutionRawHistoryScope, request.GetDomain())
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(request.GetDomain()))
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// force limit page size if exceed
	if request.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetThrottledLogger().Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowDomainID(domainID), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	// this function return the following 5 things,
	// 1. current branch token
	// 2. the workflow run ID
	// 3. the next event ID
	// 4. error if any
	queryHistory := func(
		domainUUID string,
		execution *commonproto.WorkflowExecution,
		currentBranchToken []byte,
	) ([]byte, string, int64, error) {
		response, err := wh.GetHistoryClient().GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			DomainUUID:          domainUUID,
			Execution:           execution,
			ExpectedNextEventId: common.EmptyEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, err
		}

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetNextEventId(),
			nil
	}

	execution := request.Execution
	var continuationToken *token.HistoryContinuation

	var runID string
	var nextEventID int64

	// process the token for paging
	if request.NextPageToken != nil {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.GetRunId() != continuationToken.GetRunId() {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = continuationToken.GetRunId()

	} else {
		continuationToken = &token.HistoryContinuation{}
		continuationToken.BranchToken, runID, nextEventID, err =
			queryHistory(domainID, execution, nil)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		execution.RunId = runID

		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = nextEventID
		continuationToken.PersistenceToken = nil
	}

	var history []*commonproto.DataBlob
	// return all events
	if continuationToken.GetFirstEventId() >= continuationToken.GetNextEventId() {
		return &workflowservice.GetWorkflowExecutionRawHistoryResponse{
			RawHistory:    []*commonproto.DataBlob{},
			NextPageToken: nil,
		}, nil
	}

	history, continuationToken.PersistenceToken, err = wh.getRawHistory(
		scope,
		domainID,
		*execution,
		continuationToken.GetFirstEventId(),
		continuationToken.GetNextEventId(),
		request.GetMaximumPageSize(),
		continuationToken.PersistenceToken,
		continuationToken.TransientDecision,
		continuationToken.BranchToken,
	)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if len(continuationToken.PersistenceToken) == 0 {
		// meaning, there is no more history to be returned
		continuationToken = nil
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &workflowservice.GetWorkflowExecutionRawHistoryResponse{
		RawHistory:    history,
		NextPageToken: nextToken,
	}, nil
}

// GetClusterInfo return information about Temporal deployment.
func (wh *WorkflowHandler) GetClusterInfo(ctx context.Context, _ *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendClientGetClusterInfoScope)
	if ok := wh.allow(""); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	return &workflowservice.GetClusterInfoResponse{
		SupportedClientVersions: &commonproto.SupportedClientVersions{
			GoSdk:   headers.SupportedGoSDKVersion,
			JavaSdk: headers.SupportedJavaSDKVersion,
		},
	}, nil
}

// ListTaskListPartitions returns all the partition and host for a task list.
func (wh *WorkflowHandler) ListTaskListPartitions(ctx context.Context, request *workflowservice.ListTaskListPartitionsRequest) (_ *workflowservice.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListTaskListPartitionsScope, request.GetDomain())
	defer sw.Stop()

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request.GetDomain()); !ok {
		return nil, wh.error(errServiceBusy, scope)
	}

	if request.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	matchingResponse, err := wh.GetMatchingClient().ListTaskListPartitions(ctx, &matchingservice.ListTaskListPartitionsRequest{
		Domain:   request.GetDomain(),
		TaskList: request.TaskList,
	})

	if matchingResponse == nil {
		return nil, err
	}

	return &workflowservice.ListTaskListPartitionsResponse{
		ActivityTaskListPartitions: matchingResponse.ActivityTaskListPartitions,
		DecisionTaskListPartitions: matchingResponse.DecisionTaskListPartitions,
	}, err
}

func (wh *WorkflowHandler) getRawHistory(
	scope metrics.Scope,
	domainID string,
	execution commonproto.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *commonproto.TransientDecisionInfo,
	branchToken []byte,
) ([]*commonproto.DataBlob, []byte, error) {
	var rawHistory []*commonproto.DataBlob
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowId(), wh.config.NumHistoryShards)

	resp, err := wh.GetHistoryManager().ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
	})
	if err != nil {
		return nil, nil, err
	}

	var encoding enums.EncodingType
	for _, data := range resp.HistoryEventBlobs {
		switch data.Encoding {
		case common.EncodingTypeJSON:
			encoding = enums.EncodingTypeJSON
		case common.EncodingTypeThriftRW:
			encoding = enums.EncodingTypeThriftRW
		default:
			panic(fmt.Sprintf("Invalid encoding type for raw history, encoding type: %s", data.Encoding))
		}
		rawHistory = append(rawHistory, &commonproto.DataBlob{
			EncodingType: encoding,
			Data:         data.Data,
		})
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}

		blob, err := wh.GetPayloadSerializer().SerializeEvent(transientDecision.ScheduledEvent, common.EncodingTypeProto3)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob.Data,
		})

		blob, err = wh.GetPayloadSerializer().SerializeEvent(transientDecision.StartedEvent, common.EncodingTypeProto3)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob.Data,
		})
	}

	return rawHistory, resp.NextPageToken, nil
}

func (wh *WorkflowHandler) getHistory(
	scope metrics.Scope,
	domainID string,
	execution commonproto.WorkflowExecution,
	firstEventID, nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *commonproto.TransientDecisionInfo,
	branchToken []byte,
) (*commonproto.History, []byte, error) {

	var size int

	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowId(), wh.config.NumHistoryShards)
	var err error
	var historyEvents []*commonproto.HistoryEvent
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageV2Events(wh.GetHistoryManager(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
	})
	if err != nil {
		return nil, nil, err
	}

	scope.RecordTimer(metrics.HistorySize, time.Duration(size))

	isLastPage := len(nextPageToken) == 0
	if err := wh.verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
		wh.GetLogger().Error("getHistory: incomplete history",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
		return nil, nil, err
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &commonproto.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) validateTransientDecisionEvents(
	expectedNextEventID int64,
	decision *commonproto.TransientDecisionInfo,
) error {

	if decision.ScheduledEvent.GetEventId() == expectedNextEventID &&
		decision.StartedEvent.GetEventId() == expectedNextEventID+1 {
		return nil
	}

	return fmt.Errorf("invalid transient decision: expectedScheduledEventID=%v expectedStartedEventID=%v but have scheduledEventID=%v startedEventID=%v",
		expectedNextEventID,
		expectedNextEventID+1,
		decision.ScheduledEvent.GetEventId(),
		decision.StartedEvent.GetEventId())
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandler) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
	// timer should be emitted with the all tag
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

// startRequestProfileWithDomain initiates recording of request metrics and returns a domain tagged scope
func (wh *WorkflowHandler) startRequestProfileWithDomain(scope int, domain string) (metrics.Scope, metrics.Stopwatch) {
	var metricsScope metrics.Scope
	if domain != "" {
		metricsScope = wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainTag(domain))
	} else {
		metricsScope = wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
	}
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

// getDefaultScope returns a default scope to use for request metrics
func (wh *WorkflowHandler) getDefaultScope(scope int) metrics.Scope {
	return wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
}

func (wh *WorkflowHandler) error(err error, scope metrics.Scope, tagsForErrorLog ...tag.Tag) error {
	switch err := err.(type) {
	case *serviceerror.Internal, *serviceerror.DataLoss:
		wh.GetLogger().WithTags(tagsForErrorLog...).Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.ServiceFailures)
		return err
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
		return err
	case *serviceerror.DomainNotActive:
		scope.IncCounter(metrics.ServiceErrDomainNotActiveCounter)
		return err
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.ServiceErrResourceExhaustedCounter)
		return err
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.ServiceErrNotFoundCounter)
		return err
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		scope.IncCounter(metrics.ServiceErrExecutionAlreadyStartedCounter)
		return err
	case *serviceerror.DomainAlreadyExists:
		scope.IncCounter(metrics.ServiceErrDomainAlreadyExistsCounter)
		return err
	case *serviceerror.CancellationAlreadyRequested:
		scope.IncCounter(metrics.ServiceErrCancellationAlreadyRequestedCounter)
		return err
	case *serviceerror.QueryFailed:
		scope.IncCounter(metrics.ServiceErrQueryFailedCounter)
		return err
	case *serviceerror.ClientVersionNotSupported:
		scope.IncCounter(metrics.ServiceErrClientVersionNotSupportedCounter)
		return err
	case *serviceerror.DeadlineExceeded:
		scope.IncCounter(metrics.ServiceErrContextTimeoutCounter)
		return err
	}

	wh.GetLogger().WithTags(tagsForErrorLog...).Error("Unknown error", tag.Error(err))
	scope.IncCounter(metrics.ServiceFailures)

	return err
}

func (wh *WorkflowHandler) validateTaskList(t *commonproto.TaskList, scope metrics.Scope) error {
	if t == nil || t.GetName() == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	if len(t.GetName()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errTaskListTooLong, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateExecutionAndEmitMetrics(w *commonproto.WorkflowExecution, scope metrics.Scope) error {
	err := validateExecution(w)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	matchingResp *matchingservice.PollForDecisionTaskResponse,
	branchToken []byte,
) (*workflowservice.PollForDecisionTaskResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no decision task to be send to worker / caller
		return &workflowservice.PollForDecisionTaskResponse{}, nil
	}

	var history *commonproto.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &commonproto.History{
			Events: []*commonproto.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}
		domain, dErr := wh.GetDomainCache().GetDomainByID(domainID)
		if dErr != nil {
			return nil, dErr
		}
		scope = scope.Tagged(metrics.DomainTag(domain.GetInfo().Name))
		history, persistenceToken, err = wh.getHistory(
			scope,
			domainID,
			*matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(domain.GetInfo().Name)),
			nil,
			matchingResp.GetDecisionInfo(),
			branchToken,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&token.HistoryContinuation{
				RunId:             matchingResp.WorkflowExecution.GetRunId(),
				FirstEventId:      firstEventID,
				NextEventId:       nextEventID,
				PersistenceToken:  persistenceToken,
				TransientDecision: matchingResp.GetDecisionInfo(),
				BranchToken:       branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &workflowservice.PollForDecisionTaskResponse{
		TaskToken:                 matchingResp.TaskToken,
		WorkflowExecution:         matchingResp.WorkflowExecution,
		WorkflowType:              matchingResp.WorkflowType,
		PreviousStartedEventId:    matchingResp.PreviousStartedEventId,
		StartedEventId:            matchingResp.StartedEventId,
		Query:                     matchingResp.Query,
		BacklogCountHint:          matchingResp.BacklogCountHint,
		Attempt:                   matchingResp.Attempt,
		History:                   history,
		NextPageToken:             continuation,
		WorkflowExecutionTaskList: matchingResp.WorkflowExecutionTaskList,
		ScheduledTimestamp:        matchingResp.ScheduledTimestamp,
		StartedTimestamp:          matchingResp.StartedTimestamp,
		Queries:                   matchingResp.Queries,
	}

	return resp, nil
}

func (wh *WorkflowHandler) verifyHistoryIsComplete(
	events []*commonproto.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // at least one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLoss(fmt.Sprintf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID))
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return serviceerror.NewDataLoss(fmt.Sprintf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEventID,
		lastEventID,
		nEvents,
		isFirstPage,
		isLastPage,
		pageSize))
}

func (wh *WorkflowHandler) isFailoverRequest(updateRequest *workflowservice.UpdateDomainRequest) bool {
	return updateRequest.ReplicationConfiguration != nil && updateRequest.ReplicationConfiguration.GetActiveClusterName() != ""
}

func (wh *WorkflowHandler) historyArchived(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest, domainID string) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunId() == "" {
		return false
	}
	getMutableStateRequest := &historyservice.GetMutableStateRequest{
		DomainUUID: domainID,
		Execution:  request.Execution,
	}
	_, err := wh.GetHistoryClient().GetMutableState(ctx, getMutableStateRequest)
	if err == nil {
		return false
	}
	switch err.(type) {
	case *serviceerror.NotFound:
		// the only case in which history is assumed to be archived is if getting mutable state returns entity not found error
		return true
	}

	return false
}

func (wh *WorkflowHandler) getArchivedHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	domainID string,
	scope metrics.Scope,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	entry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	URIString := entry.GetConfig().HistoryArchivalURI
	if URIString == "" {
		// if URI is empty, it means the domain has never enabled for archival.
		// the error is not "workflow has passed retention period", because
		// we have no way to tell if the requested workflow exists or not.
		return nil, wh.error(errHistoryNotFound, scope)
	}

	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	historyArchiver, err := wh.GetArchiverProvider().GetHistoryArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp, err := historyArchiver.Get(ctx, URI, &archiver.GetHistoryRequest{
		DomainID:      domainID,
		WorkflowID:    request.GetExecution().GetWorkflowId(),
		RunID:         request.GetExecution().GetRunId(),
		NextPageToken: request.GetNextPageToken(),
		PageSize:      int(request.GetMaximumPageSize()),
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	history := &commonproto.History{}
	for _, batch := range resp.HistoryBatches {
		history.Events = append(history.Events, batch.Events...)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      true,
	}, nil
}

func (wh *WorkflowHandler) convertIndexedKeyToProto(keys map[string]interface{}) map[string]enums.IndexedValueType {
	converted := make(map[string]enums.IndexedValueType)
	for k, v := range keys {
		converted[k] = common.ConvertIndexedValueTypeToProtoType(v, wh.GetLogger())
	}
	return converted
}

func (wh *WorkflowHandler) isListRequestPageSizeTooLarge(pageSize int32, domain string) bool {
	return wh.config.EnableReadVisibilityFromES(domain) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
}

func (wh *WorkflowHandler) allow(domain string) bool {
	return wh.rateLimiter.Allow(quotas.Info{Domain: domain})
}
func (wh *WorkflowHandler) checkPermission(
	config *Config,
	securityToken string,
) error {
	if config.EnableAdminProtection() {
		if securityToken == "" {
			return errNoPermission
		}
		requiredToken := config.AdminOperationToken()
		if securityToken != requiredToken {
			return errNoPermission
		}
	}
	return nil
}

func (wh *WorkflowHandler) cancelOutstandingPoll(ctx context.Context, err error, domainID string, taskListType int32,
	taskList *commonproto.TaskList, pollerID string) error {
	// First check if this err is due to context cancellation.  This means client connection to frontend is closed.
	if ctx.Err() == context.Canceled {
		// Our rpc stack does not propagates context cancellation to the other service.  Lets make an explicit
		// call to matching to notify this poller is gone to prevent any tasks being dispatched to zombie pollers.
		_, err = wh.GetMatchingClient().CancelOutstandingPoll(context.Background(), &matchingservice.CancelOutstandingPollRequest{
			DomainUUID:   domainID,
			TaskListType: taskListType,
			TaskList:     taskList,
			PollerID:     pollerID,
		})
		// We can not do much if this call fails.  Just log the error and move on
		if err != nil {
			wh.GetLogger().Warn("Failed to cancel outstanding poller.",
				tag.WorkflowTaskListName(taskList.GetName()), tag.Error(err))
		}

		// Clear error as we don't want to report context cancellation error to count against our SLA
		return nil
	}

	return err
}

func (wh *WorkflowHandler) checkBadBinary(domainEntry *cache.DomainCacheEntry, binaryChecksum string) error {
	if domainEntry.GetConfig().BadBinaries.Binaries != nil {
		badBinaries := domainEntry.GetConfig().BadBinaries.Binaries
		_, ok := badBinaries[binaryChecksum]
		if ok {
			wh.GetMetricsClient().IncCounter(metrics.FrontendPollForDecisionTaskScope, metrics.ServiceErrBadBinaryCounter)
			return serviceerror.NewInvalidArgument(fmt.Sprintf("Binary %v already marked as bad deployment.", binaryChecksum))
		}
	}
	return nil
}
