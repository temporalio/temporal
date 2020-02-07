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
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/status"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/grpc/codes"

	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/adapter"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/client"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/elasticsearch/validator"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/resource"
)

var _ workflowservice.WorkflowServiceServer = (*WorkflowHandlerGRPC)(nil)

type (
	// WorkflowHandlerGRPC - gRPC handler interface for workflow workflowservice
	WorkflowHandlerGRPC struct {
		resource.Resource

		workflowHandlerThrift *WorkflowHandler

		tokenSerializer           common.TaskTokenSerializer
		rateLimiter               quotas.Policy
		config                    *Config
		versionChecker            client.VersionChecker
		domainHandler             domain.Handler
		visibilityQueryValidator  *validator.VisibilityQueryValidator
		searchAttributesValidator *validator.SearchAttributesValidator
	}

	getHistoryContinuationTokenGRPC struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *commonproto.TransientDecisionInfo
		BranchToken       []byte
		ReplicationInfo   map[string]*commonproto.ReplicationInfo
	}
)

var (
	stDomainNotSet                               = status.New(codes.InvalidArgument, "Domain not set on request.")
	stTaskTokenNotSet                            = status.New(codes.InvalidArgument, "Task token not set on request.")
	stInvalidTaskToken                           = status.New(codes.InvalidArgument, "Invalid TaskToken.")
	stTaskListNotSet                             = status.New(codes.InvalidArgument, "TaskList is not set on request.")
	stTaskListTypeNotSet                         = status.New(codes.InvalidArgument, "TaskListType is not set on request.")
	stExecutionNotSet                            = status.New(codes.InvalidArgument, "Execution is not set on request.")
	stWorkflowIDNotSet                           = status.New(codes.InvalidArgument, "WorkflowId is not set on request.")
	stActivityIDNotSet                           = status.New(codes.InvalidArgument, "ActivityID is not set on request.")
	stInvalidRunID                               = status.New(codes.InvalidArgument, "Invalid RunId.")
	stInvalidNextPageToken                       = status.New(codes.InvalidArgument, "Invalid NextPageToken.")
	stNextPageTokenRunIDMismatch                 = status.New(codes.InvalidArgument, "RunID in the request does not match the NextPageToken.")
	stQueryNotSet                                = status.New(codes.InvalidArgument, "WorkflowQuery is not set on request.")
	stQueryTypeNotSet                            = status.New(codes.InvalidArgument, "QueryType is not set on request.")
	stRequestNotSet                              = status.New(codes.InvalidArgument, "Request is nil.")
	stNoPermission                               = status.New(codes.PermissionDenied, "No permission to do this operation.")
	stRequestIDNotSet                            = status.New(codes.InvalidArgument, "RequestId is not set on request.")
	stWorkflowTypeNotSet                         = status.New(codes.InvalidArgument, "WorkflowType is not set on request.")
	stInvalidRetention                           = status.New(codes.InvalidArgument, "RetentionDays is invalid.")
	stInvalidExecutionStartToCloseTimeoutSeconds = status.New(codes.InvalidArgument, "A valid ExecutionStartToCloseTimeoutSeconds is not set on request.")
	stInvalidTaskStartToCloseTimeoutSeconds      = status.New(codes.InvalidArgument, "A valid TaskStartToCloseTimeoutSeconds is not set on request.")
	stQueryDisallowedForDomain                   = status.New(codes.InvalidArgument, "Domain is not allowed to query, please contact cadence team to re-enable queries.")
	stClusterNameNotSet                          = status.New(codes.InvalidArgument, "Cluster name is not set.")
	stEmptyReplicationInfo                       = status.New(codes.InvalidArgument, "Replication task info is not set.")
	stHistoryNotFound                            = status.New(codes.InvalidArgument, "Requested workflow history not found, may have passed retention period.")
	stDomainTooLong                              = status.New(codes.InvalidArgument, "Domain length exceeds limit.")
	stWorkflowTypeTooLong                        = status.New(codes.InvalidArgument, "WorkflowType length exceeds limit.")
	stWorkflowIDTooLong                          = status.New(codes.InvalidArgument, "WorkflowID length exceeds limit.")
	stSignalNameTooLong                          = status.New(codes.InvalidArgument, "SignalName length exceeds limit.")
	stTaskListTooLong                            = status.New(codes.InvalidArgument, "TaskList length exceeds limit.")
	stRequestIDTooLong                           = status.New(codes.InvalidArgument, "RequestID length exceeds limit.")
	stIdentityTooLong                            = status.New(codes.InvalidArgument, "Identity length exceeds limit.")
)

// NewWorkflowHandlerGRPC creates a gRPC handler for the cadence workflowservice
func NewWorkflowHandlerGRPC(
	resource resource.Resource,
	workflowHandlerThrift *WorkflowHandler,
	config *Config,
	replicationMessageSink messaging.Producer,
) *WorkflowHandlerGRPC {
	handler := &WorkflowHandlerGRPC{
		Resource:              resource,
		workflowHandlerThrift: workflowHandlerThrift,
		config:                config,
		tokenSerializer:       common.NewJSONTaskTokenSerializer(),
		rateLimiter: quotas.NewMultiStageRateLimiter(
			func() float64 {
				return float64(config.RPS())
			},
			func(domain string) float64 {
				return float64(config.DomainRPS(domain))
			},
		),
		versionChecker: client.NewVersionChecker(),
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
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, request *workflowservice.RegisterDomainRequest) (_ *workflowservice.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RegisterDomainResponse{}, nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) DescribeDomain(ctx context.Context, request *workflowservice.DescribeDomainRequest) (_ *workflowservice.DescribeDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.DescribeDomain(ctx, adapter.ToThriftDescribeDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeDomainResponse(response), nil
}

// ListDomains returns the information and configuration for all domains.
func (wh *WorkflowHandlerGRPC) ListDomains(ctx context.Context, request *workflowservice.ListDomainsRequest) (_ *workflowservice.ListDomainsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListDomains(ctx, adapter.ToThriftListDomainsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListDomainsResponse(response), nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandlerGRPC) UpdateDomain(ctx context.Context, request *workflowservice.UpdateDomainRequest) (_ *workflowservice.UpdateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.UpdateDomain(ctx, adapter.ToThriftUpdateDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoUpdateDomainResponse(response), nil
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandlerGRPC) DeprecateDomain(ctx context.Context, request *workflowservice.DeprecateDomainRequest) (_ *workflowservice.DeprecateDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.DeprecateDomain(ctx, adapter.ToThriftDeprecateDomainRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.DeprecateDomainResponse{}, nil
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first DecisionTask for the worker to make the
// first decision for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowHandlerGRPC) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.StartWorkflowExecution(ctx, adapter.ToThriftStartWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoStartWorkflowExecutionResponse(response), nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
// execution in unknown to the service.
func (wh *WorkflowHandlerGRPC) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetWorkflowExecutionHistory(ctx, adapter.ToThriftGetWorkflowExecutionHistoryRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetWorkflowExecutionHistoryResponse(response), nil
}

// PollForDecisionTask is called by application worker to process DecisionTask from a specific taskList.  A
// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
// Application is then expected to call 'RespondDecisionTaskCompleted' API when it is done processing the DecisionTask.
// It will also create a 'DecisionTaskStarted' event in the history for that session before handing off DecisionTask to
// application worker.
func (wh *WorkflowHandlerGRPC) PollForDecisionTask(ctx context.Context, request *workflowservice.PollForDecisionTaskRequest) (_ *workflowservice.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	tagsForErrorLog := []tag.Tag{tag.WorkflowDomainName(request.GetDomain())}
	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForDecisionTaskScope, request)
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

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientErrorGRPC)
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
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskCompleted(ctx context.Context, request *workflowservice.RespondDecisionTaskCompletedRequest) (_ *workflowservice.RespondDecisionTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RespondDecisionTaskCompleted(ctx, adapter.ToThriftRespondDecisionTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRespondDecisionTaskCompletedResponse(response), nil
}

// RespondDecisionTaskFailed is called by application worker to indicate failure.  This results in
// DecisionTaskFailedEvent written to the history and a new DecisionTask created.  This API can be used by client to
// either clear sticky tasklist or report any panics during DecisionTask processing.  Cadence will only append first
// DecisionTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowHandlerGRPC) RespondDecisionTaskFailed(ctx context.Context, request *workflowservice.RespondDecisionTaskFailedRequest) (_ *workflowservice.RespondDecisionTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondDecisionTaskFailed(ctx, adapter.ToThriftRespondDecisionTaskFailedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
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
func (wh *WorkflowHandlerGRPC) PollForActivityTask(ctx context.Context, request *workflowservice.PollForActivityTaskRequest) (_ *workflowservice.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForActivityTaskScope, request)
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

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientErrorGRPC)
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
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RecordActivityTaskHeartbeat(ctx, adapter.ToThriftRecordActivityTaskHeartbeatRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRecordActivityTaskHeartbeatResponse(response), nil
}

// RecordActivityTaskHeartbeatByID is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatByID' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Domain, WorkflowID and ActivityID
func (wh *WorkflowHandlerGRPC) RecordActivityTaskHeartbeatByID(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIDRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.RecordActivityTaskHeartbeatByID(ctx, adapter.ToThriftRecordActivityTaskHeartbeatByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoRecordActivityTaskHeartbeatByIDResponse(response), nil
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompleted(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedRequest) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCompleted(ctx, adapter.ToThriftRespondActivityTaskCompletedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskCompletedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new DecisionTask
// created for the workflow so new decisions could be made.  Similar to RespondActivityTaskCompleted but use Domain,
// WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCompletedByID(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIDRequest) (_ *workflowservice.RespondActivityTaskCompletedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCompletedByID(ctx, adapter.ToThriftRespondActivityTaskCompletedByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCompletedByIDResponse{}, nil
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailed(ctx context.Context, request *workflowservice.RespondActivityTaskFailedRequest) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskFailed(ctx, adapter.ToThriftRespondActivityTaskFailedRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskFailedByID is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskFailed but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskFailedByID(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIDRequest) (_ *workflowservice.RespondActivityTaskFailedByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskFailedByID(ctx, adapter.ToThriftRespondActivityTaskFailedByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskFailedByIDResponse{}, nil
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Use the 'taskToken' provided as response of
// PollForActivityTask API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCanceled(ctx, adapter.ToThriftRespondActivityTaskCanceledRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondActivityTaskCanceledByID is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made.  Similar to RespondActivityTaskCanceled but use
// Domain, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandlerGRPC) RespondActivityTaskCanceledByID(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIDRequest) (_ *workflowservice.RespondActivityTaskCanceledByIDResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RespondActivityTaskCanceledByID(ctx, adapter.ToThriftRespondActivityTaskCanceledByIDRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RespondActivityTaskCanceledByIDResponse{}, nil
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new DecisionTask
// created for the workflow instance so new decisions could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowHandlerGRPC) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RequestCancelWorkflowExecution(ctx, adapter.ToThriftRequestCancelWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandlerGRPC) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.SignalWorkflowExecution(ctx, adapter.ToThriftSignalWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a decision task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandlerGRPC) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.SignalWithStartWorkflowExecution(ctx, adapter.ToThriftSignalWithStartWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoSignalWithStartWorkflowExecutionResponse(response), nil
}

// ResetWorkflowExecution reset an existing workflow execution to DecisionTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandlerGRPC) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ResetWorkflowExecution(ctx, adapter.ToThriftResetWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoResetWorkflowExecutionResponse(response), nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandlerGRPC) TerminateWorkflowExecution(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.TerminateWorkflowExecution(ctx, adapter.ToThriftTerminateWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListOpenWorkflowExecutions(ctx, adapter.ToThriftListOpenWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListOpenWorkflowExecutionsResponse(response), nil
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListClosedWorkflowExecutions(ctx, adapter.ToThriftListClosedWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListClosedWorkflowExecutionsResponse(response), nil
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListWorkflowExecutions(ctx, adapter.ToThriftListWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListWorkflowExecutionsResponse(response), nil
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ListArchivedWorkflowExecutions(ctx, adapter.ToThriftListArchivedWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoListArchivedWorkflowExecutionsResponse(response), nil
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific domain without order.
func (wh *WorkflowHandlerGRPC) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.ScanWorkflowExecutions(ctx, adapter.ToThriftScanWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoScanWorkflowExecutionsResponse(response), nil
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific domain.
func (wh *WorkflowHandlerGRPC) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.CountWorkflowExecutions(ctx, adapter.ToThriftCountWorkflowExecutionsRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoCountWorkflowExecutionsResponse(response), nil
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandlerGRPC) GetSearchAttributes(ctx context.Context, _ *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetSearchAttributes(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetSearchAttributesResponse(response), nil
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a DecisionTask for query)
// as a result of 'PollForDecisionTask' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowHandlerGRPC) RespondQueryTaskCompleted(ctx context.Context, request *workflowservice.RespondQueryTaskCompletedRequest) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondQueryTaskCompletedScope)
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	if request.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if queryTaskToken.DomainID == "" || queryTaskToken.TaskList == "" || queryTaskToken.TaskID == "" {
		return nil, wh.error(errInvalidTaskToken, scope)
	}

	domainEntry, err := wh.GetDomainCache().GetDomainByID(queryTaskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondQueryTaskCompletedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(request.GetQueryResult()),
		sizeLimitWarn,
		sizeLimitError,
		queryTaskToken.DomainID,
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

	headers := client.GetHeadersValue(ctx, common.ClientImplHeaderName, common.FeatureVersionHeaderName)
	request.WorkerVersionInfo = &commonproto.WorkerVersionInfo{
		Impl:           headers[0],
		FeatureVersion: headers[1],
	}
	matchingRequest := &matchingservice.RespondQueryTaskCompletedRequest{
		DomainUUID:       queryTaskToken.DomainID,
		TaskList:         &commonproto.TaskList{Name: queryTaskToken.TaskList},
		TaskID:           queryTaskToken.TaskID,
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
func (wh *WorkflowHandlerGRPC) ResetStickyTaskList(ctx context.Context, request *workflowservice.ResetStickyTaskListRequest) (_ *workflowservice.ResetStickyTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	_, err := wh.workflowHandlerThrift.ResetStickyTaskList(ctx, adapter.ToThriftResetStickyTaskListRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &workflowservice.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandlerGRPC) QueryWorkflow(ctx context.Context, request *workflowservice.QueryWorkflowRequest) (_ *workflowservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.QueryWorkflow(ctx, adapter.ToThriftQueryWorkflowRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoQueryWorkflowResponse(response), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandlerGRPC) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.DescribeWorkflowExecution(ctx, adapter.ToThriftDescribeWorkflowExecutionRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeWorkflowExecutionResponse(response), nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes.
func (wh *WorkflowHandlerGRPC) DescribeTaskList(ctx context.Context, request *workflowservice.DescribeTaskListRequest) (_ *workflowservice.DescribeTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendDescribeTaskListScope, request)
	defer sw.Stop()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
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

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientErrorGRPC)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return &workflowservice.DescribeTaskListResponse{
		Pollers:        matchingResponse.Pollers,
		TaskListStatus: matchingResponse.TaskListStatus,
	}, nil
}

// GetWorkflowExecutionRawHistory retrieves raw history directly from DB layer.
func (wh *WorkflowHandlerGRPC) GetWorkflowExecutionRawHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionRawHistoryRequest) (_ *workflowservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetWorkflowExecutionRawHistory(ctx, adapter.ToThriftGetWorkflowExecutionRawHistoryRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetWorkflowExecutionRawHistoryResponse(response), nil
}

// GetClusterInfo return information about Temporal deployment.
func (wh *WorkflowHandlerGRPC) GetClusterInfo(ctx context.Context, _ *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	response, err := wh.workflowHandlerThrift.GetClusterInfo(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetClusterInfoResponse(response), nil
}

// ListTaskListPartitions returns all the partition and host for a task list.
func (wh *WorkflowHandlerGRPC) ListTaskListPartitions(ctx context.Context, request *workflowservice.ListTaskListPartitionsRequest) (_ *workflowservice.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListTaskListPartitionsScope, request)
	defer sw.Stop()

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(request); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
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

//===============================================================================
func (wh *WorkflowHandlerGRPC) getRawHistory(
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
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}

		blob, err := wh.GetPayloadSerializer().SerializeEvent(adapter.ToThriftHistoryEvent(transientDecision.ScheduledEvent), common.EncodingTypeThriftRW)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob.Data,
		})

		blob, err = wh.GetPayloadSerializer().SerializeEvent(adapter.ToThriftHistoryEvent(transientDecision.StartedEvent), common.EncodingTypeThriftRW)
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

func (wh *WorkflowHandlerGRPC) getHistory(
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
	var persistenceHistoryEvents []*shared.HistoryEvent
	persistenceHistoryEvents, size, nextPageToken, err = persistence.ReadFullPageV2Events(wh.GetHistoryManager(), &persistence.ReadHistoryBranchRequest{
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

	historyEvents := adapter.ToProtoHistoryEvents(persistenceHistoryEvents)

	isLastPage := len(nextPageToken) == 0
	if err := wh.verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
		wh.GetLogger().Error("getHistory: incomplete history",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
		return nil, nil, err
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
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

func (wh *WorkflowHandlerGRPC) validateTransientDecisionEvents(
	expectedNextEventID int64,
	decision *commonproto.TransientDecisionInfo,
) error {

	if decision.ScheduledEvent.GetEventId() == expectedNextEventID &&
		decision.StartedEvent.GetEventId() == expectedNextEventID+1 {
		return nil
	}

	return fmt.Errorf(
		"invalid transient decision: "+
			"expectedScheduledEventID=%v expectedStartedEventID=%v but have scheduledEventID=%v startedEventID=%v",
		expectedNextEventID,
		expectedNextEventID+1,
		decision.ScheduledEvent.GetEventId(),
		decision.StartedEvent.GetEventId())
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandlerGRPC) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
	// timer should be emitted with the all tag
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

// startRequestProfileWithDomain initiates recording of request metrics and returns a domain tagged scope
func (wh *WorkflowHandlerGRPC) startRequestProfileWithDomain(scope int, d domainGetter) (metrics.Scope, metrics.Stopwatch) {
	var metricsScope metrics.Scope
	if d != nil {
		metricsScope = wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainTag(d.GetDomain()))
	} else {
		metricsScope = wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
	}
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

// getDefaultScope returns a default scope to use for request metrics
func (wh *WorkflowHandlerGRPC) getDefaultScope(scope int) metrics.Scope {
	return wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
}

func (wh *WorkflowHandlerGRPC) error(err error, scope metrics.Scope, tagsForErrorLog ...tag.Tag) error {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded {
			scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
		}
		scope.IncCounter(metrics.CadenceFailures)
		return err
	}

	return adapter.ToProtoError(wh.errorThrift(err, scope, tagsForErrorLog...))
}

func (wh *WorkflowHandlerGRPC) errorThrift(err error, scope metrics.Scope, tagsForErrorLog ...tag.Tag) error {
	switch err := err.(type) {
	case *shared.InternalServiceError:
		wh.GetLogger().WithTags(tagsForErrorLog...).Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return frontendInternalServiceError("cadence internal error, msg: %v", err.Message)
	case *shared.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *shared.DomainNotActiveError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *shared.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return err
	case *shared.EntityNotExistsError:
		scope.IncCounter(metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *shared.WorkflowExecutionAlreadyStartedError:
		scope.IncCounter(metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *shared.DomainAlreadyExistsError:
		scope.IncCounter(metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	case *shared.CancellationAlreadyRequestedError:
		scope.IncCounter(metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return err
	case *shared.QueryFailedError:
		scope.IncCounter(metrics.CadenceErrQueryFailedCounter)
		return err
	case *shared.LimitExceededError:
		scope.IncCounter(metrics.CadenceErrLimitExceededCounter)
		return err
	case *shared.ClientVersionNotSupportedError:
		scope.IncCounter(metrics.CadenceErrClientVersionNotSupportedCounter)
		return err
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			return err
		}
	}

	wh.GetLogger().WithTags(tagsForErrorLog...).Error("Uncategorized error",
		tag.Error(err))
	scope.IncCounter(metrics.CadenceFailures)
	return frontendInternalServiceError("cadence internal uncategorized error, msg: %v", err.Error())
}

func (wh *WorkflowHandlerGRPC) validateTaskList(t *commonproto.TaskList, scope metrics.Scope) error {
	if t == nil || t.GetName() == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	if len(t.GetName()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errTaskListTooLong, scope)
	}
	return nil
}

func (wh *WorkflowHandlerGRPC) validateExecutionAndEmitMetrics(w *commonproto.WorkflowExecution, scope metrics.Scope) error {
	err := wh.validateExecution(w)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func (wh *WorkflowHandlerGRPC) validateExecution(w *commonproto.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}

func (wh *WorkflowHandlerGRPC) createPollForDecisionTaskResponse(
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
			continuation, err = wh.serializeHistoryToken(&getHistoryContinuationTokenGRPC{
				RunID:             matchingResp.WorkflowExecution.GetRunId(),
				FirstEventID:      firstEventID,
				NextEventID:       nextEventID,
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

func (wh *WorkflowHandlerGRPC) verifyHistoryIsComplete(
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
		return fmt.Errorf("invalid history: contains zero events")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // atleast one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return status.New(codes.Internal, fmt.Sprintf(
				"invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID)).Err()
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

	return status.New(codes.Internal,
		fmt.Sprintf(
			"incomplete history: "+
				"expected events [%v-%v] but got events [%v-%v] of length %v:"+
				"isFirstPage=%v,isLastPage=%v,pageSize=%v",
			expectedFirstEventID,
			expectedLastEventID,
			firstEventID,
			lastEventID,
			nEvents,
			isFirstPage,
			isLastPage,
			pageSize),
	).Err()
}

func (wh *WorkflowHandlerGRPC) deserializeHistoryToken(bytes []byte) (*getHistoryContinuationTokenGRPC, error) {
	token := &getHistoryContinuationTokenGRPC{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func (wh *WorkflowHandlerGRPC) serializeHistoryToken(token *getHistoryContinuationTokenGRPC) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func (wh *WorkflowHandlerGRPC) isFailoverRequest(updateRequest *workflowservice.UpdateDomainRequest) bool {
	return updateRequest.ReplicationConfiguration != nil && updateRequest.ReplicationConfiguration.GetActiveClusterName() != ""
}

func (wh *WorkflowHandlerGRPC) historyArchived(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest, domainID string) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunId() == "" {
		return false
	}
	getMutableStateRequest := &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  adapter.ToThriftWorkflowExecution(request.Execution),
	}
	_, err := wh.GetHistoryClient().GetMutableState(ctx, getMutableStateRequest, versionHeaders(ctx)...)
	if err == nil {
		return false
	}
	switch err.(type) {
	case *shared.EntityNotExistsError:
		// the only case in which history is assumed to be archived is if getting mutable state returns entity not found error
		return true
	}
	return false
}

func (wh *WorkflowHandlerGRPC) getArchivedHistory(
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
		history.Events = append(history.Events, adapter.ToProtoHistoryEvents(batch.Events)...)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      true,
	}, nil
}

func (wh *WorkflowHandlerGRPC) convertIndexedKeyToProto(keys map[string]interface{}) map[string]enums.IndexedValueType {
	converted := make(map[string]enums.IndexedValueType)
	for k, v := range keys {
		converted[k] = common.ConvertIndexedValueTypeToProtoType(v, wh.GetLogger())
	}
	return converted
}

func (wh *WorkflowHandlerGRPC) isListRequestPageSizeTooLarge(pageSize int32, domain string) bool {
	return wh.config.EnableReadVisibilityFromES(domain) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
}

func (wh *WorkflowHandlerGRPC) allow(d domainGetter) bool {
	domain := ""
	if d != nil {
		domain = d.GetDomain()
	}
	return wh.rateLimiter.Allow(quotas.Info{Domain: domain})
}
func (wh *WorkflowHandlerGRPC) checkPermission(
	config *Config,
	securityToken *string,
) error {
	if config.EnableAdminProtection() {
		if securityToken == nil || *securityToken == "" {
			return errNoPermission
		}
		requiredToken := config.AdminOperationToken()
		if *securityToken != requiredToken {
			return errNoPermission
		}
	}
	return nil
}

func (wh *WorkflowHandlerGRPC) cancelOutstandingPoll(ctx context.Context, err error, domainID string, taskListType int32,
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

func (wh *WorkflowHandlerGRPC) checkBadBinary(domainEntry *cache.DomainCacheEntry, binaryChecksum string) error {
	if domainEntry.GetConfig().BadBinaries.Binaries != nil {
		badBinaries := domainEntry.GetConfig().BadBinaries.Binaries
		_, ok := badBinaries[binaryChecksum]
		if ok {
			wh.GetMetricsClient().IncCounter(metrics.FrontendPollForDecisionTaskScope, metrics.CadenceErrBadBinaryCounter)
			return status.New(codes.InvalidArgument, fmt.Sprintf("binary %v already marked as bad deployment", binaryChecksum)).Err()
		}
	}
	return nil
}
