// Copyright (c) 2017 Uber Technologies, Inc.
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
	"sync"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

var _ workflowserviceserver.Interface = (*WorkflowHandler)(nil)

type (
	// WorkflowHandler - Thrift handler inteface for workflow service
	WorkflowHandler struct {
		domainCache        cache.DomainCache
		metadataMgr        persistence.MetadataManager
		historyMgr         persistence.HistoryManager
		visibitiltyMgr     persistence.VisibilityManager
		history            history.Client
		matching           matching.Client
		tokenSerializer    common.TaskTokenSerializer
		hSerializerFactory persistence.HistorySerializerFactory
		metricsClient      metrics.Client
		startWG            sync.WaitGroup
		rateLimiter        common.TokenBucket
		config             *Config
		service.Service
	}

	getHistoryContinuationToken struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *gen.TransientDecisionInfo
	}
)

var (
	errDomainNotSet               = &gen.BadRequestError{Message: "Domain not set on request."}
	errTaskTokenNotSet            = &gen.BadRequestError{Message: "Task token not set on request."}
	errInvalidTaskToken           = &gen.BadRequestError{Message: "Invalid TaskToken."}
	errInvalidRequestType         = &gen.BadRequestError{Message: "Invalid request type."}
	errTaskListNotSet             = &gen.BadRequestError{Message: "TaskList is not set on request."}
	errExecutionNotSet            = &gen.BadRequestError{Message: "Execution is not set on request."}
	errWorkflowIDNotSet           = &gen.BadRequestError{Message: "WorkflowId is not set on request."}
	errRunIDNotSet                = &gen.BadRequestError{Message: "RunId is not set on request."}
	errActivityIDNotSet           = &gen.BadRequestError{Message: "ActivityID is not set on request."}
	errInvalidRunID               = &gen.BadRequestError{Message: "Invalid RunId."}
	errInvalidNextPageToken       = &gen.BadRequestError{Message: "Invalid NextPageToken."}
	errNextPageTokenRunIDMismatch = &gen.BadRequestError{Message: "RunID in the request does not match the NextPageToken."}
	errQueryNotSet                = &gen.BadRequestError{Message: "WorkflowQuery is not set on request."}
	errQueryTypeNotSet            = &gen.BadRequestError{Message: "QueryType is not set on request."}
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(
	sVice service.Service, config *Config, metadataMgr persistence.MetadataManager,
	historyMgr persistence.HistoryManager, visibilityMgr persistence.VisibilityManager) *WorkflowHandler {
	handler := &WorkflowHandler{
		Service:            sVice,
		config:             config,
		metadataMgr:        metadataMgr,
		historyMgr:         historyMgr,
		visibitiltyMgr:     visibilityMgr,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		domainCache:        cache.NewDomainCache(metadataMgr, sVice.GetLogger()),
		rateLimiter:        common.NewTokenBucket(config.RPS, common.NewRealTimeSource()),
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler
}

// Start starts the handler
func (wh *WorkflowHandler) Start() error {
	wh.Service.GetDispatcher().Register(workflowserviceserver.New(wh))
	wh.Service.GetDispatcher().Register(metaserver.New(wh))
	wh.Service.Start()
	var err error
	wh.history, err = wh.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	wh.matching, err = wh.Service.GetClientFactory().NewMatchingClient()
	if err != nil {
		return err
	}
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	return nil
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	wh.metadataMgr.Close()
	wh.visibitiltyMgr.Close()
	wh.historyMgr.Close()
	wh.Service.Stop()
}

// Health is for health check
func (wh *WorkflowHandler) Health(ctx context.Context) (*health.HealthStatus, error) {
	wh.startWG.Wait()
	wh.GetLogger().Debug("Frontend health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("frontend good")}
	return hs, nil
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, registerRequest *gen.RegisterDomainRequest) error {

	scope := metrics.FrontendRegisterDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if registerRequest.Name == nil || *registerRequest.Name == "" {
		return wh.error(errDomainNotSet, scope)
	}

	response, err := wh.metadataMgr.CreateDomain(&persistence.CreateDomainRequest{
		Name:        *registerRequest.Name,
		Status:      persistence.DomainStatusRegistered,
		OwnerEmail:  common.StringDefault(registerRequest.OwnerEmail),
		Description: common.StringDefault(registerRequest.Description),
		Retention:   common.Int32Default(registerRequest.WorkflowExecutionRetentionPeriodInDays),
		EmitMetric:  common.BoolDefault(registerRequest.EmitMetric),
	})

	if err != nil {
		return wh.error(err, scope)
	}

	// TODO: Log through logging framework.  We need to have good auditing of domain CRUD
	wh.GetLogger().Debugf("Register domain succeeded for name: %v, Id: %v", *registerRequest.Name, response.ID)
	return nil
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(ctx context.Context,
	describeRequest *gen.DescribeDomainRequest) (*gen.DescribeDomainResponse, error) {

	scope := metrics.FrontendDescribeDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if describeRequest.Name == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	resp, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: *describeRequest.Name,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	response := &gen.DescribeDomainResponse{}
	response.DomainInfo, response.Configuration = createDomainResponse(resp.Info, resp.Config)

	return response, nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(ctx context.Context,
	updateRequest *gen.UpdateDomainRequest) (*gen.UpdateDomainResponse, error) {

	scope := metrics.FrontendUpdateDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if updateRequest.Name == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	getResponse, err0 := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: *updateRequest.Name,
	})

	if err0 != nil {
		return nil, wh.error(err0, scope)
	}

	info := getResponse.Info
	config := getResponse.Config

	if updateRequest.UpdatedInfo != nil {
		updatedInfo := updateRequest.UpdatedInfo
		if updatedInfo.Description != nil {
			info.Description = *updatedInfo.Description
		}
		if updatedInfo.OwnerEmail != nil {
			info.OwnerEmail = *updatedInfo.OwnerEmail
		}
	}

	if updateRequest.Configuration != nil {
		updatedConfig := updateRequest.Configuration
		if updatedConfig.EmitMetric != nil {
			config.EmitMetric = *updatedConfig.EmitMetric
		}
		if updatedConfig.WorkflowExecutionRetentionPeriodInDays != nil {
			config.Retention = *updatedConfig.WorkflowExecutionRetentionPeriodInDays
		}
	}

	err := wh.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:   info,
		Config: config,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	response := &gen.UpdateDomainResponse{}
	response.DomainInfo, response.Configuration = createDomainResponse(info, config)
	return response, nil
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED.  Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, deprecateRequest *gen.DeprecateDomainRequest) error {

	scope := metrics.FrontendDeprecateDomainScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if deprecateRequest.Name == nil {
		return wh.error(errDomainNotSet, scope)
	}

	getResponse, err0 := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: *deprecateRequest.Name,
	})

	if err0 != nil {
		return wh.error(err0, scope)
	}

	info := getResponse.Info
	info.Status = persistence.DomainStatusDeprecated
	config := getResponse.Config

	err := wh.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:   info,
		Config: config,
	})
	if err != nil {
		return wh.error(errDomainNotSet, scope)
	}
	return nil
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx context.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {

	scope := metrics.FrontendPollForActivityTaskScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	if pollRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}

	info, _, err := wh.domainCache.GetDomain(*pollRequest.Domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	pollerID := uuid.New()
	resp, err := wh.matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{
		DomainUUID:  common.StringPtr(info.ID),
		PollerID:    common.StringPtr(pollerID),
		PollRequest: pollRequest,
	})
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, info.ID, persistence.TaskListTypeActivity, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			wh.Service.GetLogger().Errorf(
				"PollForActivityTask failed. TaskList: %v, Error: %v", *pollRequest.TaskList.Name, err)
			return nil, wh.error(err, scope)
		}
	}
	return resp, nil
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx context.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {

	scope := metrics.FrontendPollForDecisionTaskScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	if pollRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}

	domainName := *pollRequest.Domain
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debugf("Poll for decision. DomainName: %v, DomainID: %v", domainName, info.ID)

	pollerID := uuid.New()
	matchingResp, err := wh.matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
		DomainUUID:  common.StringPtr(info.ID),
		PollerID:    common.StringPtr(pollerID),
		PollRequest: pollRequest,
	})
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, info.ID, persistence.TaskListTypeDecision, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			wh.Service.GetLogger().Errorf(
				"PollForDecisionTask failed. TaskList: %v, Error: %v", *pollRequest.TaskList.Name, err)
			return nil, wh.error(err, scope)
		}

		// Must be cancellation error.  Does'nt matter what we return here.  Client already went away.
		return nil, nil
	}

	resp, err := wh.createPollForDecisionTaskResponse(ctx, info.ID, matchingResp)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

func (wh *WorkflowHandler) cancelOutstandingPoll(ctx context.Context, err error, domainID string, taskListType int32,
	taskList *gen.TaskList, pollerID string) error {
	// First check if this err is due to context cancellation.  This means client connection to frontend is closed.
	if ctx.Err() == context.Canceled {
		// Our rpc stack does not propagates context cancellation to the other service.  Lets make an explicit
		// call to matching to notify this poller is gone to prevent any tasks being dispatched to zombie pollers.
		err = wh.matching.CancelOutstandingPoll(context.Background(), &m.CancelOutstandingPollRequest{
			DomainUUID:   common.StringPtr(domainID),
			TaskListType: common.Int32Ptr(taskListType),
			TaskList:     taskList,
			PollerID:     common.StringPtr(pollerID),
		})
		// We can do much if this call fails.  Just log the error and move on
		if err != nil {
			wh.Service.GetLogger().Warnf("Failed to cancel outstanding poller.  Tasklist: %v, Error: %v,",
				taskList.GetName(), err)
		}

		// Clear error as we don't want to report context cancellation error to count against our SLA
		return nil
	}

	return err
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {

	scope := metrics.FrontendRecordActivityTaskHeartbeatScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	if heartbeatRequest.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	resp, err := wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
		DomainUUID:       common.StringPtr(taskToken.DomainID),
		HeartbeatRequest: heartbeatRequest,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {

	scope := metrics.FrontendRespondActivityTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskCompletedByID - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskCompletedByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID := completeRequest.GetDomainID()
	workflowID := completeRequest.GetWorkflowID()
	runID := completeRequest.GetRunID() // runID is optional so can be empty
	activityID := completeRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	req := &gen.RespondActivityTaskCompletedRequest{
		TaskToken: token,
		Result:    completeRequest.Result,
		Identity:  completeRequest.Identity,
	}

	err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: req,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondActivityTaskFailedRequest) error {

	scope := metrics.FrontendRespondActivityTaskFailedByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if failedRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskFailedByID - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailedByID(
	ctx context.Context,
	failedRequest *gen.RespondActivityTaskFailedByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskFailedByIDScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID := failedRequest.GetDomainID()
	workflowID := failedRequest.GetWorkflowID()
	runID := failedRequest.GetRunID() // runID is optional so can be empty
	activityID := failedRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	req := &gen.RespondActivityTaskFailedRequest{
		TaskToken: token,
		Reason:    failedRequest.Reason,
		Details:   failedRequest.Details,
		Identity:  failedRequest.Identity,
	}

	err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: req,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskCanceled - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx context.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {

	scope := metrics.FrontendRespondActivityTaskCanceledScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if cancelRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		CancelRequest: cancelRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondActivityTaskCanceledByID - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	cancelRequest *gen.RespondActivityTaskCanceledByIDRequest) error {

	scope := metrics.FrontendRespondActivityTaskCanceledScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	domainID := cancelRequest.GetDomainID()
	workflowID := cancelRequest.GetWorkflowID()
	runID := cancelRequest.GetRunID() // runID is optional so can be empty
	activityID := cancelRequest.GetActivityID()

	if domainID == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return wh.error(errActivityIDNotSet, scope)
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return wh.error(err, scope)
	}

	req := &gen.RespondActivityTaskCanceledRequest{
		TaskToken: token,
		Details:   cancelRequest.Details,
		Identity:  cancelRequest.Identity,
	}

	err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		CancelRequest: req,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {

	scope := metrics.FrontendRespondDecisionTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	err = wh.history.RespondDecisionTaskCompleted(ctx, &h.RespondDecisionTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest},
	)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondDecisionTaskFailed - failed response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondDecisionTaskFailedRequest) error {

	scope := metrics.FrontendRespondDecisionTaskFailedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if failedRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return wh.error(errDomainNotSet, scope)
	}

	err = wh.history.RespondDecisionTaskFailed(ctx, &h.RespondDecisionTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// RespondQueryTaskCompleted - response to a query task
func (wh *WorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondQueryTaskCompletedRequest) error {

	scope := metrics.FrontendRespondQueryTaskCompletedScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.rateLimiter.TryConsume(1)

	if completeRequest.TaskToken == nil {
		return wh.error(errTaskTokenNotSet, scope)
	}
	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(completeRequest.TaskToken)
	if err != nil {
		return wh.error(err, scope)
	}
	if queryTaskToken.DomainID == "" || queryTaskToken.TaskList == "" || queryTaskToken.TaskID == "" {
		return wh.error(errInvalidTaskToken, scope)
	}

	matchingRequest := &m.RespondQueryTaskCompletedRequest{
		DomainUUID:       common.StringPtr(queryTaskToken.DomainID),
		TaskList:         &gen.TaskList{Name: common.StringPtr(queryTaskToken.TaskList)},
		TaskID:           common.StringPtr(queryTaskToken.TaskID),
		CompletedRequest: completeRequest,
	}

	err = wh.matching.RespondQueryTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// StartWorkflowExecution - Creates a new workflow execution
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {

	scope := metrics.FrontendStartWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if startRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if startRequest.WorkflowId == nil || *startRequest.WorkflowId == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowId is not set on request."}, scope)
	}

	wh.Service.GetLogger().Debugf(
		"Received StartWorkflowExecution. WorkflowID: %v",
		*startRequest.WorkflowId)

	if startRequest.WorkflowType == nil ||
		startRequest.WorkflowType.Name == nil || *startRequest.WorkflowType.Name == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowType is not set on request."}, scope)
	}

	if err := wh.validateTaskList(startRequest.TaskList, scope); err != nil {
		return nil, err
	}

	if startRequest.ExecutionStartToCloseTimeoutSeconds == nil ||
		*startRequest.ExecutionStartToCloseTimeoutSeconds <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	if startRequest.TaskStartToCloseTimeoutSeconds == nil ||
		*startRequest.TaskStartToCloseTimeoutSeconds <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	domainName := *startRequest.Domain
	wh.Service.GetLogger().Debugf("Start workflow execution request domain: %v", domainName)
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debugf("Start workflow execution request domainID: %v", info.ID)

	resp, err := wh.history.StartWorkflowExecution(ctx, &h.StartWorkflowExecutionRequest{
		DomainUUID:   common.StringPtr(info.ID),
		StartRequest: startRequest,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {

	scope := metrics.FrontendGetWorkflowExecutionHistoryScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if getRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecution(getRequest.Execution, scope); err != nil {
		return nil, err
	}

	if getRequest.MaximumPageSize == nil || *getRequest.MaximumPageSize == 0 {
		getRequest.MaximumPageSize = common.Int32Ptr(wh.config.DefaultHistoryMaxPageSize)
	}

	domainInfo, _, err := wh.domainCache.GetDomain(*getRequest.Domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// this function return the following 3 things,
	// 1. the workflow run ID
	// 2. the last first event ID (the event ID of the last batch of events in the history)
	// 3. the next event ID
	// 4. whether the workflow is closed
	queryHistory := func(domainUUID string, execution *gen.WorkflowExecution, expectedNextEventID int64) (string, int64, int64, bool, error) {
		response, err := wh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
			DomainUUID:          common.StringPtr(domainUUID),
			Execution:           execution,
			ExpectedNextEventId: common.Int64Ptr(expectedNextEventID),
		})

		if err != nil {
			return "", 0, 0, false, err
		}
		return response.Execution.GetRunId(), response.GetLastFirstEventId(), response.GetNextEventId(), response.GetIsWorkflowRunning(), nil
	}

	isLongPoll := getRequest.GetWaitForNewEvent()
	isCloseEventOnly := getRequest.GetHistoryEventFilterType() == gen.HistoryEventFilterTypeCloseEvent
	execution := getRequest.Execution
	token := &getHistoryContinuationToken{}

	var runID string
	var lastFirstEventID int64
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if getRequest.NextPageToken != nil {
		token, err = deserializeHistoryToken(getRequest.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.RunId != nil && *execution.RunId != token.RunID {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = common.StringPtr(token.RunID)

		// we need to update the current next event ID and whether workflow is running
		if len(token.PersistenceToken) == 0 && isLongPoll && token.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = token.NextEventID
			}
			_, lastFirstEventID, nextEventID, isWorkflowRunning, err = queryHistory(domainInfo.ID, execution, queryNextEventID)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			token.FirstEventID = token.NextEventID
			token.NextEventID = nextEventID
			token.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		runID, lastFirstEventID, nextEventID, isWorkflowRunning, err = queryHistory(domainInfo.ID, execution, queryNextEventID)
		if err != nil {
			return nil, wh.error(err, scope)
		}

		execution.RunId = &runID

		token.RunID = runID
		token.FirstEventID = common.FirstEventID
		token.NextEventID = nextEventID
		token.IsWorkflowRunning = isWorkflowRunning
		token.PersistenceToken = nil
	}

	history := &gen.History{}
	history.Events = []*gen.HistoryEvent{}
	if isCloseEventOnly {
		if !isWorkflowRunning {
			history, _, err = wh.getHistory(domainInfo.ID, *execution, lastFirstEventID, nextEventID,
				*getRequest.MaximumPageSize, nil, token.TransientDecision)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			// since getHistory func will not return empty history, so the below is safe
			history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			token = nil
		} else if isLongPoll {
			// set the persistance token to be nil so next time we will query history for updates
			token.PersistenceToken = nil
		} else {
			token = nil
		}
	} else {
		// return all events
		if token.FirstEventID >= token.NextEventID {
			// currently there is no new event
			history.Events = []*gen.HistoryEvent{}
			if !isWorkflowRunning {
				token = nil
			}
		} else {
			history, token.PersistenceToken, err =
				wh.getHistory(domainInfo.ID, *execution, token.FirstEventID, token.NextEventID,
					*getRequest.MaximumPageSize, token.PersistenceToken, token.TransientDecision)
			if err != nil {
				return nil, wh.error(err, scope)
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(token.PersistenceToken) == 0 && (!token.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				token = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(token)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return createGetWorkflowExecutionHistoryResponse(history, nextToken), nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context,
	signalRequest *gen.SignalWorkflowExecutionRequest) error {

	scope := metrics.FrontendSignalWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if signalRequest.Domain == nil {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecution(signalRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	if signalRequest.SignalName == nil {
		return wh.error(&gen.BadRequestError{Message: "SignalName is not set on request."}, scope)
	}

	info, _, err := wh.domainCache.GetDomain(*signalRequest.Domain)
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.SignalWorkflowExecution(ctx, &h.SignalWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(info.ID),
		SignalRequest: signalRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(ctx context.Context,
	terminateRequest *gen.TerminateWorkflowExecutionRequest) error {

	scope := metrics.FrontendTerminateWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if terminateRequest.Domain == nil {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecution(terminateRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	info, _, err := wh.domainCache.GetDomain(*terminateRequest.Domain)
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.TerminateWorkflowExecution(ctx, &h.TerminateWorkflowExecutionRequest{
		DomainUUID:       common.StringPtr(info.ID),
		TerminateRequest: terminateRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// RequestCancelWorkflowExecution - requests to cancel a workflow execution
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	cancelRequest *gen.RequestCancelWorkflowExecutionRequest) error {

	scope := metrics.FrontendRequestCancelWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if cancelRequest.Domain == nil {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecution(cancelRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	info, _, err := wh.domainCache.GetDomain(*cancelRequest.Domain)
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.RequestCancelWorkflowExecution(ctx, &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(info.ID),
		CancelRequest: cancelRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// ListOpenWorkflowExecutions - retrieves info for open workflow executions in a domain
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context,
	listRequest *gen.ListOpenWorkflowExecutionsRequest) (*gen.ListOpenWorkflowExecutionsResponse, error) {

	scope := metrics.FrontendListOpenWorkflowExecutionsScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.StartTimeFilter == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.ExecutionFilter != nil && listRequest.TypeFilter != nil {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed"}, scope)
	}

	if listRequest.MaximumPageSize == nil || *listRequest.MaximumPageSize == 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(wh.config.DefaultVisibilityMaxPageSize)
	}

	domainInfo, _, err := wh.domainCache.GetDomain(*listRequest.Domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainInfo.ID,
		PageSize:          int(*listRequest.MaximumPageSize),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: *listRequest.StartTimeFilter.EarliestTime,
		LatestStartTime:   *listRequest.StartTimeFilter.LatestTime,
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByWorkflowID(
			&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowID:                    *listRequest.ExecutionFilter.WorkflowId,
			})
	} else if listRequest.TypeFilter != nil {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              *listRequest.TypeFilter.Name,
		})
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &gen.ListOpenWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListClosedWorkflowExecutions - retrieves info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(ctx context.Context,
	listRequest *gen.ListClosedWorkflowExecutionsRequest) (*gen.ListClosedWorkflowExecutionsResponse, error) {

	scope := metrics.FrontendListClosedWorkflowExecutionsScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.StartTimeFilter == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}, scope)
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, wh.error(&gen.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}, scope)
	}

	filterCount := 0
	if listRequest.ExecutionFilter != nil {
		filterCount++
	}
	if listRequest.TypeFilter != nil {
		filterCount++
	}
	if listRequest.StatusFilter != nil {
		filterCount++
	}

	if filterCount > 1 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"}, scope)
	}

	if listRequest.MaximumPageSize == nil || *listRequest.MaximumPageSize == 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(wh.config.DefaultVisibilityMaxPageSize)
	}

	domainInfo, _, err := wh.domainCache.GetDomain(*listRequest.Domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainInfo.ID,
		PageSize:          int(*listRequest.MaximumPageSize),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: *listRequest.StartTimeFilter.EarliestTime,
		LatestStartTime:   *listRequest.StartTimeFilter.LatestTime,
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByWorkflowID(
			&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowID:                    *listRequest.ExecutionFilter.WorkflowId,
			})
	} else if listRequest.TypeFilter != nil {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              *listRequest.TypeFilter.Name,
		})
	} else if listRequest.StatusFilter != nil {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByStatus(&persistence.ListClosedWorkflowExecutionsByStatusRequest{
			ListWorkflowExecutionsRequest: baseReq,
			Status: *listRequest.StatusFilter,
		})
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp := &gen.ListClosedWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(ctx context.Context,
	queryRequest *gen.QueryWorkflowRequest) (*gen.QueryWorkflowResponse, error) {

	scope := metrics.FrontendQueryWorkflowScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if queryRequest.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if queryRequest.Execution == nil {
		return nil, wh.error(errExecutionNotSet, scope)
	}

	if queryRequest.Execution.WorkflowId == nil {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}

	if queryRequest.Execution.RunId != nil && uuid.Parse(*queryRequest.Execution.RunId) == nil {
		return nil, wh.error(errInvalidRunID, scope)
	}

	if queryRequest.Query == nil {
		return nil, wh.error(errQueryNotSet, scope)
	}

	if queryRequest.Query.QueryType == nil {
		return nil, wh.error(errQueryTypeNotSet, scope)
	}

	domainInfo, _, err := wh.domainCache.GetDomain(queryRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	matchingRequest := &m.QueryWorkflowRequest{
		DomainUUID:   common.StringPtr(domainInfo.ID),
		QueryRequest: queryRequest,
	}

	// we should always use the mutable state, since it contains the sticky tasklist information
	response, err := wh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainInfo.ID),
		Execution:  queryRequest.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	clientFeature := client.NewFeatureImpl(
		response.GetClientLibraryVersion(),
		response.GetClientFeatureVersion(),
		response.GetClientImpl(),
	)

	queryRequest.Execution.RunId = response.Execution.RunId
	if len(response.StickyTaskList.GetName()) == 0 {
		matchingRequest.TaskList = response.TaskList
	} else if !clientFeature.SupportStickyQuery() {
		// sticky enabled on the client side, but client chose to use non sticky, which is also the default
		matchingRequest.TaskList = response.TaskList
	} else {
		matchingRequest.TaskList = response.StickyTaskList
	}

	matchingResp, err := wh.matching.QueryWorkflow(ctx, matchingRequest)
	if err != nil {
		logging.LogQueryTaskFailedEvent(wh.GetLogger(),
			*queryRequest.Domain,
			*queryRequest.Execution.WorkflowId,
			*queryRequest.Execution.RunId,
			*queryRequest.Query.QueryType)
		return nil, wh.error(err, scope)
	}

	return matchingResp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(ctx context.Context, request *gen.DescribeWorkflowExecutionRequest) (*gen.DescribeWorkflowExecutionResponse, error) {

	scope := metrics.FrontendDescribeWorkflowExecutionScope
	sw := wh.startRequestProfile(scope)
	defer sw.Stop()

	if ok, _ := wh.rateLimiter.TryConsume(1); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if request.Domain == nil {
		return nil, wh.error(errDomainNotSet, scope)
	}
	domainInfo, _, err := wh.domainCache.GetDomain(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateExecution(request.Execution, scope); err != nil {
		return nil, err
	}

	response, err := wh.history.DescribeWorkflowExecution(ctx, &h.DescribeWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainInfo.ID),
		Request:    request,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return response, nil
}

func (wh *WorkflowHandler) getHistory(domainID string, execution gen.WorkflowExecution,
	firstEventID, nextEventID int64, pageSize int32, nextPageToken []byte,
	transientDecision *gen.TransientDecisionInfo) (*gen.History, []byte, error) {

	historyEvents := []*gen.HistoryEvent{}

	response, err := wh.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     execution,
		FirstEventID:  firstEventID,
		NextEventID:   nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
	})

	if err != nil {
		return nil, nil, err
	}

	for _, e := range response.Events {
		setSerializedHistoryDefaults(&e)
		s, _ := wh.hSerializerFactory.Get(e.EncodingType)
		history, err1 := s.Deserialize(&e)
		if err1 != nil {
			return nil, nil, err1
		}
		historyEvents = append(historyEvents, history.Events...)
	}

	nextPageToken = response.NextPageToken
	if len(nextPageToken) == 0 && transientDecision != nil {
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &gen.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

// sets the version and encoding types to defaults if they
// are missing from persistence. This is purely for backwards
// compatibility
func setSerializedHistoryDefaults(history *persistence.SerializedHistoryEventBatch) {
	if history.Version == 0 {
		history.Version = persistence.GetDefaultHistoryVersion()
	}
	if len(history.EncodingType) == 0 {
		history.EncodingType = persistence.DefaultEncodingType
	}
}

func (wh *WorkflowHandler) getLoggerForTask(taskToken []byte) bark.Logger {
	logger := wh.Service.GetLogger()
	task, err := wh.tokenSerializer.Deserialize(taskToken)
	if err == nil {
		logger = logger.WithFields(bark.Fields{
			"WorkflowID": task.WorkflowID,
			"RunID":      task.RunID,
			"ScheduleID": task.ScheduleID,
		})
	}
	return logger
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandler) startRequestProfile(scope int) tally.Stopwatch {
	wh.startWG.Wait()
	sw := wh.metricsClient.StartTimer(scope, metrics.CadenceLatency)
	wh.metricsClient.IncCounter(scope, metrics.CadenceRequests)
	return sw
}

func (wh *WorkflowHandler) error(err error, scope int) error {
	switch err.(type) {
	case *gen.InternalServiceError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceFailures)
		return err
	case *gen.BadRequestError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.ServiceBusyError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrServiceBusyCounter)
		return err
	case *gen.EntityNotExistsError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	case *gen.CancellationAlreadyRequestedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return err
	case *gen.QueryFailedError:
		wh.metricsClient.IncCounter(scope, metrics.CadenceErrQueryFailedCounter)
		return err
	default:
		wh.metricsClient.IncCounter(scope, metrics.CadenceFailures)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}

func (wh *WorkflowHandler) validateTaskList(t *gen.TaskList, scope int) error {
	if t == nil || t.Name == nil || *t.Name == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateExecution(w *gen.WorkflowExecution, scope int) error {
	if w == nil {
		return wh.error(errExecutionNotSet, scope)
	}
	if w.WorkflowId == nil || *w.WorkflowId == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if w.RunId != nil && uuid.Parse(*w.RunId) == nil {
		return wh.error(errInvalidRunID, scope)
	}
	return nil
}

func getDomainStatus(info *persistence.DomainInfo) *gen.DomainStatus {
	switch info.Status {
	case persistence.DomainStatusRegistered:
		v := gen.DomainStatusRegistered
		return &v
	case persistence.DomainStatusDeprecated:
		v := gen.DomainStatusDeprecated
		return &v
	case persistence.DomainStatusDeleted:
		v := gen.DomainStatusDeleted
		return &v
	}

	return nil
}

func createDomainResponse(info *persistence.DomainInfo, config *persistence.DomainConfig) (*gen.DomainInfo,
	*gen.DomainConfiguration) {

	i := &gen.DomainInfo{}
	i.Name = common.StringPtr(info.Name)
	i.Status = getDomainStatus(info)
	i.Description = common.StringPtr(info.Description)
	i.OwnerEmail = common.StringPtr(info.OwnerEmail)

	c := &gen.DomainConfiguration{}
	c.EmitMetric = common.BoolPtr(config.EmitMetric)
	c.WorkflowExecutionRetentionPeriodInDays = common.Int32Ptr(config.Retention)

	return i, c
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(ctx context.Context, domainID string,
	matchingResp *m.PollForDecisionTaskResponse) (*gen.PollForDecisionTaskResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no decision task to be send to worker / caller
		return &gen.PollForDecisionTaskResponse{}, nil
	}

	var history *gen.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &gen.History{
			Events: []*gen.HistoryEvent{},
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
		history, persistenceToken, err = wh.getHistory(
			domainID,
			*matchingResp.WorkflowExecution,
			firstEventID,
			nextEventID,
			wh.config.DefaultHistoryMaxPageSize,
			nil,
			matchingResp.DecisionInfo)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&getHistoryContinuationToken{
				RunID:             matchingResp.WorkflowExecution.GetRunId(),
				FirstEventID:      firstEventID,
				NextEventID:       nextEventID,
				PersistenceToken:  persistenceToken,
				TransientDecision: matchingResp.DecisionInfo,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &gen.PollForDecisionTaskResponse{
		TaskToken:              matchingResp.TaskToken,
		WorkflowExecution:      matchingResp.WorkflowExecution,
		WorkflowType:           matchingResp.WorkflowType,
		PreviousStartedEventId: matchingResp.PreviousStartedEventId,
		StartedEventId:         matchingResp.StartedEventId,
		Query:                  matchingResp.Query,
		BacklogCountHint:       matchingResp.BacklogCountHint,
		History:                history,
		NextPageToken:          continuation,
	}

	return resp, nil
}

func createGetWorkflowExecutionHistoryResponse(
	history *gen.History, nextPageToken []byte) *gen.GetWorkflowExecutionHistoryResponse {
	resp := &gen.GetWorkflowExecutionHistoryResponse{}
	resp.History = history
	resp.NextPageToken = nextPageToken
	return resp
}

func deserializeHistoryToken(bytes []byte) (*getHistoryContinuationToken, error) {
	token := &getHistoryContinuationToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func serializeHistoryToken(token *getHistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func createServiceBusyError() *gen.ServiceBusyError {
	err := &gen.ServiceBusyError{}
	err.Message = "Too many outstanding requests to the cadence service"
	return err
}
