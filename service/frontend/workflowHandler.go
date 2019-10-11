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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source ../../.gen/go/cadence/workflowserviceserver/server.go -destination workflowHandler_mock.go -mock_names Interface=MockWorkflowHandler

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/elasticsearch/validator"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service"
)

const (
	getDomainReplicationMessageBatchSize = 100
	defaultLastMessageID                 = -1
)

var _ workflowserviceserver.Interface = (*WorkflowHandler)(nil)

type (
	// WorkflowHandler - Thrift handler interface for workflow service
	WorkflowHandler struct {
		domainCache               cache.DomainCache
		metadataMgr               persistence.MetadataManager
		historyV2Mgr              persistence.HistoryV2Manager
		visibilityMgr             persistence.VisibilityManager
		history                   history.Client
		matching                  matching.Client
		matchingRawClient         matching.Client
		tokenSerializer           common.TaskTokenSerializer
		metricsClient             metrics.Client
		startWG                   sync.WaitGroup
		rateLimiter               quotas.Policy
		config                    *Config
		versionChecker            *versionChecker
		domainHandler             domain.Handler
		visibilityQueryValidator  *validator.VisibilityQueryValidator
		searchAttributesValidator *validator.SearchAttributesValidator
		domainReplicationQueue    persistence.DomainReplicationQueue
		service.Service
	}

	getHistoryContinuationToken struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *gen.TransientDecisionInfo
		BranchToken       []byte
		ReplicationInfo   map[string]*gen.ReplicationInfo
	}

	domainGetter interface {
		GetDomain() string
	}
)

var (
	errDomainNotSet                               = &gen.BadRequestError{Message: "Domain not set on request."}
	errTaskTokenNotSet                            = &gen.BadRequestError{Message: "Task token not set on request."}
	errInvalidTaskToken                           = &gen.BadRequestError{Message: "Invalid TaskToken."}
	errInvalidRequestType                         = &gen.BadRequestError{Message: "Invalid request type."}
	errTaskListNotSet                             = &gen.BadRequestError{Message: "TaskList is not set on request."}
	errTaskListTypeNotSet                         = &gen.BadRequestError{Message: "TaskListType is not set on request."}
	errExecutionNotSet                            = &gen.BadRequestError{Message: "Execution is not set on request."}
	errWorkflowIDNotSet                           = &gen.BadRequestError{Message: "WorkflowId is not set on request."}
	errRunIDNotSet                                = &gen.BadRequestError{Message: "RunId is not set on request."}
	errActivityIDNotSet                           = &gen.BadRequestError{Message: "ActivityID is not set on request."}
	errInvalidRunID                               = &gen.BadRequestError{Message: "Invalid RunId."}
	errInvalidNextPageToken                       = &gen.BadRequestError{Message: "Invalid NextPageToken."}
	errNextPageTokenRunIDMismatch                 = &gen.BadRequestError{Message: "RunID in the request does not match the NextPageToken."}
	errQueryNotSet                                = &gen.BadRequestError{Message: "WorkflowQuery is not set on request."}
	errQueryTypeNotSet                            = &gen.BadRequestError{Message: "QueryType is not set on request."}
	errRequestNotSet                              = &gen.BadRequestError{Message: "Request is nil."}
	errNoPermission                               = &gen.BadRequestError{Message: "No permission to do this operation."}
	errRequestIDNotSet                            = &gen.BadRequestError{Message: "RequestId is not set on request."}
	errWorkflowTypeNotSet                         = &gen.BadRequestError{Message: "WorkflowType is not set on request."}
	errInvalidExecutionStartToCloseTimeoutSeconds = &gen.BadRequestError{Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}
	errInvalidTaskStartToCloseTimeoutSeconds      = &gen.BadRequestError{Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}
	errClientVersionNotSet                        = &gen.BadRequestError{Message: "Client version is not set on request."}

	// err for archival
	errHistoryHasPassedRetentionPeriod = &gen.BadRequestError{Message: "Requested workflow history has passed retention period."}
	// the following errors represents bad user input
	errURIUpdate = &shared.BadRequestError{Message: "Cannot update existing archival URI"}

	// err for string too long
	errDomainTooLong       = &gen.BadRequestError{Message: "Domain length exceeds limit."}
	errWorkflowTypeTooLong = &gen.BadRequestError{Message: "WorkflowType length exceeds limit."}
	errWorkflowIDTooLong   = &gen.BadRequestError{Message: "WorkflowID length exceeds limit."}
	errSignalNameTooLong   = &gen.BadRequestError{Message: "SignalName length exceeds limit."}
	errTaskListTooLong     = &gen.BadRequestError{Message: "TaskList length exceeds limit."}
	errRequestIDTooLong    = &gen.BadRequestError{Message: "RequestID length exceeds limit."}
	errIdentityTooLong     = &gen.BadRequestError{Message: "Identity length exceeds limit."}

	frontendServiceRetryPolicy = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(
	sVice service.Service,
	config *Config,
	metadataMgr persistence.MetadataManager,
	historyV2Mgr persistence.HistoryV2Manager,
	visibilityMgr persistence.VisibilityManager,
	replicationMessageSink messaging.Producer,
	domainReplicationQueue persistence.DomainReplicationQueue,
	domainCache cache.DomainCache,
) *WorkflowHandler {
	handler := &WorkflowHandler{
		Service:         sVice,
		config:          config,
		metadataMgr:     metadataMgr,
		historyV2Mgr:    historyV2Mgr,
		visibilityMgr:   visibilityMgr,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		metricsClient:   sVice.GetMetricsClient(),
		domainCache:     domainCache,
		rateLimiter: quotas.NewMultiStageRateLimiter(
			func() float64 {
				return float64(config.RPS())
			},
			func(domain string) float64 {
				return float64(config.DomainRPS(domain))
			},
		),
		versionChecker: &versionChecker{checkVersion: config.EnableClientVersionCheck()},
		domainHandler: domain.NewHandler(
			config.MinRetentionDays(),
			config.MaxBadBinaries,
			sVice.GetLogger(),
			metadataMgr,
			sVice.GetClusterMetadata(),
			domain.NewDomainReplicator(replicationMessageSink, sVice.GetLogger()),
			sVice.GetArchivalMetadata(),
			sVice.GetArchiverProvider(),
		),
		visibilityQueryValidator: validator.NewQueryValidator(config.ValidSearchAttributes),
		searchAttributesValidator: validator.NewSearchAttributesValidator(
			sVice.GetLogger(),
			config.ValidSearchAttributes,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
		domainReplicationQueue: domainReplicationQueue,
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler
}

// RegisterHandler register this handler, must be called before Start()
// if DCRedirectionHandler is also used, use RegisterHandler in DCRedirectionHandler instead
func (wh *WorkflowHandler) RegisterHandler() {
	wh.Service.GetDispatcher().Register(workflowserviceserver.New(wh))
	wh.Service.GetDispatcher().Register(metaserver.New(wh))
}

// Start starts the handler
func (wh *WorkflowHandler) Start() error {
	wh.domainCache.Start()

	wh.history = wh.GetClientBean().GetHistoryClient()
	matchingRawClient, err := wh.GetClientBean().GetMatchingClient(wh.domainCache.GetDomainName)
	if err != nil {
		return err
	}
	wh.matchingRawClient = matchingRawClient
	wh.matching = matching.NewRetryableClient(wh.matchingRawClient, common.CreateMatchingServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError)
	wh.startWG.Done()
	return nil
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	wh.domainReplicationQueue.Close()
	wh.domainCache.Stop()
	wh.metadataMgr.Close()
	wh.visibilityMgr.Close()
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
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, registerRequest *gen.RegisterDomainRequest) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendRegisterDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if registerRequest == nil {
		return errRequestNotSet
	}

	if err := wh.checkPermission(registerRequest.SecurityToken); err != nil {
		return err
	}

	if registerRequest.GetName() == "" {
		return errDomainNotSet
	}

	err := wh.domainHandler.RegisterDomain(ctx, registerRequest)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

// ListDomains returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) ListDomains(
	ctx context.Context,
	listRequest *gen.ListDomainsRequest,
) (response *gen.ListDomainsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendListDomainsScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.domainHandler.ListDomains(ctx, listRequest)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(
	ctx context.Context,
	describeRequest *gen.DescribeDomainRequest,
) (response *gen.DescribeDomainResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendDescribeDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if describeRequest == nil {
		return nil, errRequestNotSet
	}

	if describeRequest.GetName() == "" && describeRequest.GetUUID() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.DescribeDomain(ctx, describeRequest)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(
	ctx context.Context,
	updateRequest *gen.UpdateDomainRequest,
) (resp *gen.UpdateDomainResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendUpdateDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if updateRequest == nil {
		return nil, errRequestNotSet
	}

	// don't require permission for failover request
	if !isFailoverRequest(updateRequest) {
		if err := wh.checkPermission(updateRequest.SecurityToken); err != nil {
			return nil, err
		}
	}

	if updateRequest.GetName() == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.domainHandler.UpdateDomain(ctx, updateRequest)
	if err != nil {
		return resp, wh.error(err, scope)
	}
	return resp, err
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED. Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, deprecateRequest *gen.DeprecateDomainRequest) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendDeprecateDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if deprecateRequest == nil {
		return errRequestNotSet
	}

	if err := wh.checkPermission(deprecateRequest.SecurityToken); err != nil {
		return err
	}

	if deprecateRequest.GetName() == "" {
		return errDomainNotSet
	}

	err := wh.domainHandler.DeprecateDomain(ctx, deprecateRequest)
	if err != nil {
		return wh.error(err, scope)
	}
	return err
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx context.Context,
	pollRequest *gen.PollForActivityTaskRequest,
) (resp *gen.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForActivityTaskScope, pollRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if pollRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForActivityTask",
		wh.Service.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	if pollRequest.Domain == nil || pollRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(pollRequest.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}
	if len(pollRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(pollRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	pollerID := uuid.New()
	op := func() error {
		var err error
		resp, err = wh.matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{
			DomainUUID:  common.StringPtr(domainID),
			PollerID:    common.StringPtr(pollerID),
			PollRequest: pollRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeActivity, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.Service.GetLogger().Error("PollForActivityTask failed.",
				tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, wh.error(err, scope)
		}
	}
	return resp, nil
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx context.Context,
	pollRequest *gen.PollForDecisionTaskRequest,
) (resp *gen.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	callTime := time.Now()

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendPollForDecisionTaskScope, pollRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if pollRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForDecisionTask",
		wh.Service.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	if pollRequest.Domain == nil || pollRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if len(pollRequest.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if len(pollRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope); err != nil {
		return nil, err
	}

	domainName := pollRequest.GetDomain()
	domainEntry, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	domainID := domainEntry.GetInfo().ID

	wh.Service.GetLogger().Debug("Poll for decision.", tag.WorkflowDomainName(domainName), tag.WorkflowDomainID(domainID))
	if err := wh.checkBadBinary(domainEntry, pollRequest.GetBinaryChecksum()); err != nil {
		return nil, wh.error(err, scope)
	}

	pollerID := uuid.New()
	var matchingResp *m.PollForDecisionTaskResponse
	op := func() error {
		var err error
		matchingResp, err = wh.matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
			DomainUUID:  common.StringPtr(domainID),
			PollerID:    common.StringPtr(pollerID),
			PollRequest: pollRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeDecision, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.Service.GetLogger().Error("PollForDecisionTask failed.",
				tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, wh.error(err, scope)
		}

		// Must be cancellation error.  Does'nt matter what we return here.  Client already went away.
		return nil, nil
	}

	resp, err = wh.createPollForDecisionTaskResponse(ctx, scope, domainID, matchingResp, matchingResp.GetBranchToken())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

func (wh *WorkflowHandler) checkBadBinary(domainEntry *cache.DomainCacheEntry, binaryChecksum string) error {
	if domainEntry.GetConfig().BadBinaries.Binaries != nil {
		badBinaries := domainEntry.GetConfig().BadBinaries.Binaries
		_, ok := badBinaries[binaryChecksum]
		if ok {
			wh.metricsClient.IncCounter(metrics.FrontendPollForDecisionTaskScope, metrics.CadenceErrBadBinaryCounter)
			return &gen.BadRequestError{
				Message: fmt.Sprintf("binary %v already marked as bad deployment", binaryChecksum),
			}
		}
	}
	return nil
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
		// We can not do much if this call fails.  Just log the error and move on
		if err != nil {
			wh.Service.GetLogger().Warn("Failed to cancel outstanding poller.",
				tag.WorkflowTaskListName(taskList.GetName()), tag.Error(err))
		}

		// Clear error as we don't want to report context cancellation error to count against our SLA
		return nil
	}

	return err
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest,
) (resp *gen.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRecordActivityTaskHeartbeatScope)

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if heartbeatRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRecordActivityTaskHeartbeatScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(heartbeatRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: heartbeatRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		resp = &gen.RecordActivityTaskHeartbeatResponse{CancelRequested: common.BoolPtr(true)}
	} else {
		resp, err = wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
			DomainUUID:       common.StringPtr(taskToken.DomainID),
			HeartbeatRequest: heartbeatRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return resp, nil
}

// RecordActivityTaskHeartbeatByID - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatByIDRequest,
) (resp *gen.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRecordActivityTaskHeartbeatByIDScope, heartbeatRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if heartbeatRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeatByID")
	domainID, err := wh.domainCache.GetDomainID(heartbeatRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}
	workflowID := heartbeatRequest.GetWorkflowID()
	runID := heartbeatRequest.GetRunID() // runID is optional so can be empty
	activityID := heartbeatRequest.GetActivityID()

	if domainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}
	if workflowID == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}
	if activityID == "" {
		return nil, wh.error(errActivityIDNotSet, scope)
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
		return nil, wh.error(err, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(heartbeatRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
		resp = &gen.RecordActivityTaskHeartbeatResponse{CancelRequested: common.BoolPtr(true)}
	} else {
		req := &gen.RecordActivityTaskHeartbeatRequest{
			TaskToken: token,
			Details:   heartbeatRequest.Details,
			Identity:  heartbeatRequest.Identity,
		}

		resp, err = wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
			DomainUUID:       common.StringPtr(taskToken.DomainID),
			HeartbeatRequest: req,
		})
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	return resp, nil
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskCompletedScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}
	if len(completeRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskCompletedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(completeRequest.Result),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: completeRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
			DomainUUID:      common.StringPtr(taskToken.DomainID),
			CompleteRequest: completeRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondActivityTaskCompletedByID - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	completeRequest *gen.RespondActivityTaskCompletedByIDRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskCompletedByIDScope, completeRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	domainID, err := wh.domainCache.GetDomainID(completeRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
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

	if len(completeRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(completeRequest.Result),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
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
	}

	return nil
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondActivityTaskFailedRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskFailedScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskFailedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	if len(failedRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
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
	failedRequest *gen.RespondActivityTaskFailedByIDRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskFailedByIDScope, failedRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	domainID, err := wh.domainCache.GetDomainID(failedRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
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
	if len(failedRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
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
	cancelRequest *gen.RespondActivityTaskCanceledRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondActivityTaskCanceledScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondActivityTaskCanceledScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	if len(cancelRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(cancelRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: cancelRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
		err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			CancelRequest: cancelRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	}

	return nil
}

// RespondActivityTaskCanceledByID - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	cancelRequest *gen.RespondActivityTaskCanceledByIDRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRespondActivityTaskCanceledScope, cancelRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	domainID, err := wh.domainCache.GetDomainID(cancelRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}
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
	if len(cancelRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainEntry.GetInfo().Name))

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(cancelRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &gen.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
			DomainUUID:    common.StringPtr(taskToken.DomainID),
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.error(err, scope)
		}
	} else {
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
	}

	return nil
}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest,
) (resp *gen.RespondDecisionTaskCompletedResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondDecisionTaskCompletedScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if completeRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

	if completeRequest.TaskToken == nil {
		return nil, wh.error(errTaskTokenNotSet, scope)
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	if taskToken.DomainID == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondDecisionTaskCompletedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	histResp, err := wh.history.RespondDecisionTaskCompleted(ctx, &h.RespondDecisionTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest},
	)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if len(completeRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errIdentityTooLong, scope)
	}

	completedResp := &gen.RespondDecisionTaskCompletedResponse{}
	if completeRequest.GetReturnNewDecisionTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := &common.TaskToken{
			DomainID:        taskToken.DomainID,
			WorkflowID:      taskToken.WorkflowID,
			RunID:           taskToken.RunID,
			ScheduleID:      histResp.StartedResponse.GetScheduledEventId(),
			ScheduleAttempt: histResp.StartedResponse.GetAttempt(),
		}
		token, _ := wh.tokenSerializer.Serialize(taskToken)
		workflowExecution := &gen.WorkflowExecution{
			WorkflowId: common.StringPtr(taskToken.WorkflowID),
			RunId:      common.StringPtr(taskToken.RunID),
		}
		matchingResp := common.CreateMatchingPollForDecisionTaskResponse(histResp.StartedResponse, workflowExecution, token)

		newDecisionTask, err := wh.createPollForDecisionTaskResponse(ctx, scope, taskToken.DomainID, matchingResp, matchingResp.GetBranchToken())
		if err != nil {
			return nil, wh.error(err, scope)
		}
		completedResp.DecisionTask = newDecisionTask
	}

	return completedResp, nil
}

// RespondDecisionTaskFailed - failed response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	failedRequest *gen.RespondDecisionTaskFailedRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondDecisionTaskFailedScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if failedRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(taskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondDecisionTaskFailedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

	if len(failedRequest.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errIdentityTooLong, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		// details exceed, we would just truncate the size for decision task failed as the details is not used anywhere by client code
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
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
	completeRequest *gen.RespondQueryTaskCompletedRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope := wh.getDefaultScope(metrics.FrontendRespondQueryTaskCompletedScope)
	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if completeRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	wh.allow(nil)

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

	domainEntry, err := wh.domainCache.GetDomainByID(queryTaskToken.DomainID)
	if err != nil {
		return wh.error(err, scope)
	}

	scope, sw := wh.startRequestProfileWithDomain(
		metrics.FrontendRespondQueryTaskCompletedScope,
		domainWrapper{
			domain: domainEntry.GetInfo().Name,
		},
	)
	defer sw.Stop()

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
	startRequest *gen.StartWorkflowExecutionRequest,
) (resp *gen.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendStartWorkflowExecutionScope, startRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if startRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(startRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	domainName := startRequest.GetDomain()
	if domainName == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(domainName) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if startRequest.GetWorkflowId() == "" {
		return nil, wh.error(errWorkflowIDNotSet, scope)
	}

	if len(startRequest.GetWorkflowId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowIDTooLong, scope)
	}

	if err := common.ValidateRetryPolicy(startRequest.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := backoff.ValidateSchedule(startRequest.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debug(
		"Received StartWorkflowExecution. WorkflowID",
		tag.WorkflowID(startRequest.GetWorkflowId()))

	if startRequest.WorkflowType == nil || startRequest.WorkflowType.GetName() == "" {
		return nil, wh.error(errWorkflowTypeNotSet, scope)
	}

	if len(startRequest.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowTypeTooLong, scope)
	}

	if err := wh.validateTaskList(startRequest.TaskList, scope); err != nil {
		return nil, err
	}

	if startRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidExecutionStartToCloseTimeoutSeconds, scope)
	}

	if startRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(errInvalidTaskStartToCloseTimeoutSeconds, scope)
	}

	if startRequest.GetRequestId() == "" {
		return nil, wh.error(errRequestIDNotSet, scope)
	}

	if len(startRequest.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errRequestIDTooLong, scope)
	}

	if err := wh.searchAttributesValidator.ValidateSearchAttributes(startRequest.SearchAttributes, domainName); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debug("Start workflow execution request domain", tag.WorkflowDomainName(domainName))
	domainID, err := wh.domainCache.GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// add domain tag to scope, so further metrics will have the domain tag
	scope = scope.Tagged(metrics.DomainTag(domainName))

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	actualSize := len(startRequest.Input)
	if startRequest.Memo != nil {
		actualSize += common.GetSizeOfMapStringToByteArray(startRequest.Memo.GetFields())
	}
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		startRequest.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	wh.Service.GetLogger().Debug("Start workflow execution request domainID", tag.WorkflowDomainID(domainID))
	resp, err = wh.history.StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(domainID, startRequest))

	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

// GetWorkflowExecutionHistory - retrieves the history of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest,
) (resp *gen.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendGetWorkflowExecutionHistoryScope, getRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if getRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(getRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if getRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(getRequest.Execution, scope); err != nil {
		return nil, err
	}

	if getRequest.GetMaximumPageSize() <= 0 {
		getRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.HistoryMaxPageSize(getRequest.GetDomain())))
	}

	domainID, err := wh.domainCache.GetDomainID(getRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// force limit page size if exceed
	if getRequest.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetThrottledLogger().Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(getRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(getRequest.Execution.GetRunId()),
			tag.WorkflowDomainID(domainID), tag.WorkflowSize(int64(getRequest.GetMaximumPageSize())))

		getRequest.MaximumPageSize = common.Int32Ptr(common.GetHistoryMaxPageSize)
	}

	enableArchivalRead := wh.GetArchivalMetadata().GetHistoryConfig().ReadEnabled()
	historyArchived := wh.historyArchived(ctx, getRequest, domainID)
	if enableArchivalRead && historyArchived {
		return wh.getArchivedHistory(ctx, getRequest, domainID, scope)
	}

	// this function return the following 5 things,
	// 1. the workflow run ID
	// 2. the last first event ID (the event ID of the last batch of events in the history)
	// 3. the next event ID
	// 4. whether the workflow is closed
	// 5. error if any
	queryHistory := func(
		domainUUID string,
		execution *gen.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, int64, bool, error) {
		response, err := wh.history.PollMutableState(ctx, &h.PollMutableStateRequest{
			DomainUUID:          common.StringPtr(domainUUID),
			Execution:           execution,
			ExpectedNextEventId: common.Int64Ptr(expectedNextEventID),
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowCloseState() == persistence.WorkflowCloseStatusNone

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			nil
	}

	isLongPoll := getRequest.GetWaitForNewEvent()
	isCloseEventOnly := getRequest.GetHistoryEventFilterType() == gen.HistoryEventFilterTypeCloseEvent
	execution := getRequest.Execution
	token := &getHistoryContinuationToken{}

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if getRequest.NextPageToken != nil {
		token, err = deserializeHistoryToken(getRequest.NextPageToken)
		if err != nil {
			return nil, wh.error(errInvalidNextPageToken, scope)
		}
		if execution.RunId != nil && execution.GetRunId() != token.RunID {
			return nil, wh.error(errNextPageTokenRunIDMismatch, scope)
		}

		execution.RunId = common.StringPtr(token.RunID)

		// we need to update the current next event ID and whether workflow is running
		if len(token.PersistenceToken) == 0 && isLongPoll && token.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = token.NextEventID
			}
			token.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err =
				queryHistory(domainID, execution, queryNextEventID, token.BranchToken)
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
		token.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err =
			queryHistory(domainID, execution, queryNextEventID, nil)
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
			history, _, err = wh.getHistory(
				scope,
				domainID,
				*execution,
				lastFirstEventID,
				nextEventID,
				getRequest.GetMaximumPageSize(),
				nil,
				token.TransientDecision,
				token.BranchToken,
			)
			if err != nil {
				return nil, wh.error(err, scope)
			}
			// since getHistory func will not return empty history, so the below is safe
			history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			token = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
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
			history, token.PersistenceToken, err = wh.getHistory(
				scope,
				domainID,
				*execution,
				token.FirstEventID,
				token.NextEventID,
				getRequest.GetMaximumPageSize(),
				token.PersistenceToken,
				token.TransientDecision,
				token.BranchToken,
			)
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
	return &gen.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: nextToken,
		Archived:      common.BoolPtr(false),
	}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(
	ctx context.Context,
	signalRequest *gen.SignalWorkflowExecutionRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendSignalWorkflowExecutionScope, signalRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if signalRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(signalRequest); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if signalRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if len(signalRequest.GetDomain()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errDomainTooLong, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(signalRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	if signalRequest.GetSignalName() == "" {
		return wh.error(&gen.BadRequestError{Message: "SignalName is not set on request."}, scope)
	}

	if len(signalRequest.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errSignalNameTooLong, scope)
	}

	if len(signalRequest.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errRequestIDTooLong, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(signalRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(signalRequest.GetDomain())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(signalRequest.GetDomain())
	if err := common.CheckEventBlobSizeLimit(
		len(signalRequest.Input),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalRequest.GetWorkflowExecution().GetWorkflowId(),
		signalRequest.GetWorkflowExecution().GetWorkflowId(),
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.SignalWorkflowExecution(ctx, &h.SignalWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(domainID),
		SignalRequest: signalRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a decision task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandler) SignalWithStartWorkflowExecution(
	ctx context.Context,
	signalWithStartRequest *gen.SignalWithStartWorkflowExecutionRequest,
) (resp *gen.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendSignalWithStartWorkflowExecutionScope, signalWithStartRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if signalWithStartRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(signalWithStartRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	domainName := signalWithStartRequest.GetDomain()
	if domainName == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if len(domainName) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errDomainTooLong, scope)
	}

	if signalWithStartRequest.GetWorkflowId() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowId is not set on request."}, scope)
	}

	if len(signalWithStartRequest.GetWorkflowId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowIDTooLong, scope)
	}

	if signalWithStartRequest.GetSignalName() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "SignalName is not set on request."}, scope)
	}

	if len(signalWithStartRequest.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errSignalNameTooLong, scope)
	}

	if signalWithStartRequest.WorkflowType == nil || signalWithStartRequest.WorkflowType.GetName() == "" {
		return nil, wh.error(&gen.BadRequestError{Message: "WorkflowType is not set on request."}, scope)
	}

	if len(signalWithStartRequest.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errWorkflowTypeTooLong, scope)
	}

	if err := wh.validateTaskList(signalWithStartRequest.TaskList, scope); err != nil {
		return nil, err
	}

	if len(signalWithStartRequest.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, wh.error(errRequestIDTooLong, scope)
	}

	if signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}, scope)
	}

	if err := common.ValidateRetryPolicy(signalWithStartRequest.RetryPolicy); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := backoff.ValidateSchedule(signalWithStartRequest.GetCronSchedule()); err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.searchAttributesValidator.ValidateSearchAttributes(signalWithStartRequest.SearchAttributes, domainName); err != nil {
		return nil, wh.error(err, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(domainName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	if err := common.CheckEventBlobSizeLimit(
		len(signalWithStartRequest.SignalInput),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalWithStartRequest.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}
	actualSize := len(signalWithStartRequest.Input) + common.GetSizeOfMapStringToByteArray(signalWithStartRequest.Memo.GetFields())
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalWithStartRequest.GetWorkflowId(),
		"",
		scope,
		wh.GetThrottledLogger(),
	); err != nil {
		return nil, wh.error(err, scope)
	}

	op := func() error {
		var err error
		resp, err = wh.history.SignalWithStartWorkflowExecution(ctx, &h.SignalWithStartWorkflowExecutionRequest{
			DomainUUID:             common.StringPtr(domainID),
			SignalWithStartRequest: signalWithStartRequest,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return resp, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(
	ctx context.Context,
	terminateRequest *gen.TerminateWorkflowExecutionRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendTerminateWorkflowExecutionScope, terminateRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if terminateRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(terminateRequest); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if terminateRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(terminateRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	domainID, err := wh.domainCache.GetDomainID(terminateRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.TerminateWorkflowExecution(ctx, &h.TerminateWorkflowExecutionRequest{
		DomainUUID:       common.StringPtr(domainID),
		TerminateRequest: terminateRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// ResetWorkflowExecution reset an existing workflow execution to the nextFirstEventID
// in the history and immediately terminating the current execution instance.
func (wh *WorkflowHandler) ResetWorkflowExecution(
	ctx context.Context,
	resetRequest *gen.ResetWorkflowExecutionRequest,
) (resp *gen.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendResetWorkflowExecutionScope, resetRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if resetRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(resetRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if resetRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(resetRequest.WorkflowExecution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.domainCache.GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp, err = wh.history.ResetWorkflowExecution(ctx, &h.ResetWorkflowExecutionRequest{
		DomainUUID:   common.StringPtr(domainID),
		ResetRequest: resetRequest,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return resp, nil
}

// RequestCancelWorkflowExecution - requests to cancel a workflow execution
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	cancelRequest *gen.RequestCancelWorkflowExecutionRequest,
) (retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendRequestCancelWorkflowExecutionScope, cancelRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if cancelRequest == nil {
		return wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(cancelRequest); !ok {
		return wh.error(createServiceBusyError(), scope)
	}

	if cancelRequest.GetDomain() == "" {
		return wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(cancelRequest.WorkflowExecution, scope); err != nil {
		return err
	}

	domainID, err := wh.domainCache.GetDomainID(cancelRequest.GetDomain())
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.RequestCancelWorkflowExecution(ctx, &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(domainID),
		CancelRequest: cancelRequest,
	})
	if err != nil {
		return wh.error(err, scope)
	}

	return nil
}

// ListOpenWorkflowExecutions - retrieves info for open workflow executions in a domain
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(
	ctx context.Context,
	listRequest *gen.ListOpenWorkflowExecutionsRequest,
) (resp *gen.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListOpenWorkflowExecutionsScope, listRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(listRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
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

	if listRequest.StartTimeFilter.GetEarliestTime() > listRequest.StartTimeFilter.GetLatestTime() {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter should not be larger than LatestTime"}, scope)
	}

	if listRequest.ExecutionFilter != nil && listRequest.TypeFilter != nil {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed"}, scope)
	}

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetMaximumPageSize(), listRequest.GetDomain()) {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}, scope)
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: listRequest.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMgr.ListOpenWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
				})
		}
		wh.Service.GetLogger().Info("List open workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMgr.ListOpenWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              listRequest.TypeFilter.GetName(),
			})
		}
		wh.Service.GetLogger().Info("List open workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.visibilityMgr.ListOpenWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp = &gen.ListOpenWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListArchivedWorkflowExecutions - retrieves archived info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListArchivedWorkflowExecutions(
	ctx context.Context,
	listRequest *gen.ListArchivedWorkflowExecutionsRequest,
) (resp *gen.ListArchivedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendClientListArchivedWorkflowExecutionsScope, listRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(listRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival() {
		return nil, wh.error(&gen.BadRequestError{Message: "Cluster is not configured for visibility archival"}, scope)
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ReadEnabled() {
		return nil, wh.error(&gen.BadRequestError{Message: "Cluster is not configured for reading archived visibility records"}, scope)
	}

	entry, err := wh.domainCache.GetDomain(listRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if entry.GetConfig().VisibilityArchivalStatus != shared.ArchivalStatusEnabled {
		return nil, wh.error(&gen.BadRequestError{Message: "Domain is not configured for visibility archival"}, scope)
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
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         listRequest.GetQuery(),
	}

	archiverResponse, err := visibilityArchiver.Query(ctx, URI, archiverRequest)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// special handling of ExecutionTime for cron or retry
	for _, execution := range archiverResponse.Executions {
		if execution.GetExecutionTime() == 0 {
			execution.ExecutionTime = common.Int64Ptr(execution.GetStartTime())
		}
	}

	return &gen.ListArchivedWorkflowExecutionsResponse{
		Executions:    archiverResponse.Executions,
		NextPageToken: archiverResponse.NextPageToken,
	}, nil
}

// ListClosedWorkflowExecutions - retrieves info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(
	ctx context.Context,
	listRequest *gen.ListClosedWorkflowExecutionsRequest,
) (resp *gen.ListClosedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListClosedWorkflowExecutionsScope, listRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(listRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
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

	if listRequest.StartTimeFilter.GetEarliestTime() > listRequest.StartTimeFilter.GetLatestTime() {
		return nil, wh.error(&gen.BadRequestError{Message: "EarliestTime in StartTimeFilter should not be larger than LatestTime"}, scope)
	}

	filterCount := 0
	if listRequest.TypeFilter != nil {
		filterCount++
	}
	if listRequest.StatusFilter != nil {
		filterCount++
	}

	if filterCount > 1 {
		return nil, wh.error(&gen.BadRequestError{
			Message: "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"}, scope)
	} // If ExecutionFilter is provided with one of TypeFilter or StatusFilter, use ExecutionFilter and ignore other filter

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetMaximumPageSize(), listRequest.GetDomain()) {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}, scope)
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainID,
		Domain:            domain,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.NextPageToken,
		EarliestStartTime: listRequest.StartTimeFilter.GetEarliestTime(),
		LatestStartTime:   listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMgr.ListClosedWorkflowExecutionsByWorkflowID(
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
				})
		}
		wh.Service.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMgr.ListClosedWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              listRequest.TypeFilter.GetName(),
			})
		}
		wh.Service.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else if listRequest.StatusFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMgr.ListClosedWorkflowExecutionsByStatus(&persistence.ListClosedWorkflowExecutionsByStatusRequest{
				ListWorkflowExecutionsRequest: baseReq,
				Status:                        listRequest.GetStatusFilter(),
			})
		}
		wh.Service.GetLogger().Info("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.visibilityMgr.ListClosedWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp = &gen.ListClosedWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListWorkflowExecutions - retrieves info for workflow executions in a domain
func (wh *WorkflowHandler) ListWorkflowExecutions(
	ctx context.Context,
	listRequest *gen.ListWorkflowExecutionsRequest,
) (resp *gen.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendListWorkflowExecutionsScope, listRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(listRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetPageSize(), listRequest.GetDomain()) {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}, scope)
	}

	if err := wh.visibilityQueryValidator.ValidateListRequestForQuery(listRequest); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.ListWorkflowExecutionsRequestV2{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         listRequest.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp = &gen.ListWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ScanWorkflowExecutions - retrieves info for large amount of workflow executions in a domain without order
func (wh *WorkflowHandler) ScanWorkflowExecutions(
	ctx context.Context,
	listRequest *gen.ListWorkflowExecutionsRequest,
) (resp *gen.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendScanWorkflowExecutionsScope, listRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if listRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(listRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if listRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = common.Int32Ptr(int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain())))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetPageSize(), listRequest.GetDomain()) {
		return nil, wh.error(&gen.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}, scope)
	}

	if err := wh.visibilityQueryValidator.ValidateListRequestForQuery(listRequest); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.ListWorkflowExecutionsRequestV2{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         listRequest.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.ScanWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp = &gen.ListWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// CountWorkflowExecutions - count number of workflow executions in a domain
func (wh *WorkflowHandler) CountWorkflowExecutions(
	ctx context.Context,
	countRequest *gen.CountWorkflowExecutionsRequest,
) (resp *gen.CountWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendCountWorkflowExecutionsScope, countRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if countRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if ok := wh.allow(countRequest); !ok {
		return nil, wh.error(createServiceBusyError(), scope)
	}

	if countRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.visibilityQueryValidator.ValidateCountRequestForQuery(countRequest); err != nil {
		return nil, wh.error(err, scope)
	}

	domain := countRequest.GetDomain()
	domainID, err := wh.domainCache.GetDomainID(domain)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	req := &persistence.CountWorkflowExecutionsRequest{
		DomainUUID: domainID,
		Domain:     domain,
		Query:      countRequest.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.CountWorkflowExecutions(req)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp = &gen.CountWorkflowExecutionsResponse{
		Count: common.Int64Ptr(persistenceResp.Count),
	}
	return resp, nil
}

// GetSearchAttributes return valid indexed keys
func (wh *WorkflowHandler) GetSearchAttributes(ctx context.Context) (resp *gen.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendGetSearchAttributesScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	keys := wh.config.ValidSearchAttributes()
	resp = &gen.GetSearchAttributesResponse{
		Keys: wh.convertIndexedKeyToThrift(keys),
	}
	return resp, nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
func (wh *WorkflowHandler) ResetStickyTaskList(
	ctx context.Context,
	resetRequest *gen.ResetStickyTaskListRequest,
) (resp *gen.ResetStickyTaskListResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendResetStickyTaskListScope, resetRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if resetRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if resetRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(resetRequest.Execution, scope); err != nil {
		return nil, err
	}

	domainID, err := wh.domainCache.GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	_, err = wh.history.ResetStickyTaskList(ctx, &h.ResetStickyTaskListRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  resetRequest.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return &gen.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(
	ctx context.Context,
	queryRequest *gen.QueryWorkflowRequest,
) (resp *gen.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendQueryWorkflowScope, queryRequest)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if queryRequest == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if queryRequest.GetDomain() == "" {
		return nil, wh.error(errDomainNotSet, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(queryRequest.Execution, scope); err != nil {
		return nil, err
	}

	if queryRequest.Query == nil {
		return nil, wh.error(errQueryNotSet, scope)
	}

	if queryRequest.Query.GetQueryType() == "" {
		return nil, wh.error(errQueryTypeNotSet, scope)
	}

	domainID, err := wh.domainCache.GetDomainID(queryRequest.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// we should always use the mutable state, since it contains the sticky tasklist information
	response, err := wh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  queryRequest.Execution,
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	// if workflow is closed and a rejection condition is given then check if query should be rejected before proceeding
	workflowCloseStatus := response.GetWorkflowCloseState()
	if workflowCloseStatus != persistence.WorkflowCloseStatusNone && queryRequest.QueryRejectCondition != nil {
		notOpenReject := queryRequest.GetQueryRejectCondition() == gen.QueryRejectConditionNotOpen
		notCompletedCleanlyReject := queryRequest.GetQueryRejectCondition() == gen.QueryRejectConditionNotCompletedCleanly &&
			workflowCloseStatus != persistence.WorkflowCloseStatusCompleted
		if notOpenReject || notCompletedCleanlyReject {
			return &gen.QueryWorkflowResponse{
				QueryResult: nil,
				QueryRejected: &gen.QueryRejected{
					CloseStatus: persistence.ToThriftWorkflowExecutionCloseStatus(
						int(response.GetWorkflowCloseState()),
					).Ptr(),
				},
			}, nil
		}
	}

	clientFeature := client.NewFeatureImpl(
		response.GetClientLibraryVersion(),
		response.GetClientFeatureVersion(),
		response.GetClientImpl(),
	)
	queryRequest.Execution.RunId = response.Execution.RunId
	if clientFeature.SupportConsistentQuery() {
		req := &h.QueryWorkflowRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  queryRequest.GetExecution(),
			Query:      queryRequest.GetQuery(),
		}
		resp, err := wh.history.QueryWorkflow(ctx, req)
		if err != nil {
			return nil, err
		}
		return &gen.QueryWorkflowResponse{
			QueryResult:   resp.GetQueryResult(),
			QueryRejected: nil,
		}, nil
	}
	return wh.queryDirectlyThroughMatching(ctx, response, clientFeature, domainID, queryRequest, scope)
}

func (wh *WorkflowHandler) queryDirectlyThroughMatching(
	ctx context.Context,
	getMutableStateResponse *h.GetMutableStateResponse,
	clientFeature client.Feature,
	domainID string,
	queryRequest *gen.QueryWorkflowRequest,
	scope metrics.Scope,
) (*gen.QueryWorkflowResponse, error) {
	matchingRequest := &m.QueryWorkflowRequest{
		DomainUUID:   common.StringPtr(domainID),
		QueryRequest: queryRequest,
	}
	if len(getMutableStateResponse.StickyTaskList.GetName()) != 0 && clientFeature.SupportStickyQuery() {
		matchingRequest.TaskList = getMutableStateResponse.StickyTaskList
		stickyDecisionTimeout := getMutableStateResponse.GetStickyTaskListScheduleToStartTimeout()
		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickyness
		stickyContext, cancel := context.WithTimeout(context.Background(), time.Duration(stickyDecisionTimeout)*time.Second)
		matchingResp, err := wh.matchingRawClient.QueryWorkflow(stickyContext, matchingRequest)
		cancel()
		if err == nil {
			return matchingResp, nil
		}
		if yarpcError, ok := err.(*yarpcerrors.Status); !ok || yarpcError.Code() != yarpcerrors.CodeDeadlineExceeded {
			wh.Service.GetLogger().Info("QueryWorkflowFailed.",
				tag.WorkflowDomainName(queryRequest.GetDomain()),
				tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
				tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
				tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
			return nil, wh.error(err, scope)
		}
		// this means sticky timeout, should try using the normal tasklist
		// we should clear the stickyness of this workflow
		wh.GetLogger().Info("QueryWorkflowTimeouted with stickyTaskList, will try nonSticky one",
			tag.WorkflowDomainName(queryRequest.GetDomain()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
			tag.WorkflowTaskListName(getMutableStateResponse.GetStickyTaskList().GetName()),
			tag.WorkflowNextEventID(getMutableStateResponse.GetNextEventId()))
		resetContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = wh.history.ResetStickyTaskList(resetContext, &h.ResetStickyTaskListRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  queryRequest.Execution,
		})
		cancel()
		if err != nil {
			return nil, wh.error(err, scope)
		}
	}

	matchingRequest.TaskList = getMutableStateResponse.TaskList
	matchingResp, err := wh.matching.QueryWorkflow(ctx, matchingRequest)
	if err != nil {
		wh.Service.GetLogger().Info("QueryWorkflowFailed.",
			tag.WorkflowDomainName(queryRequest.GetDomain()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
		return nil, wh.error(err, scope)
	}

	return matchingResp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(
	ctx context.Context,
	request *gen.DescribeWorkflowExecutionRequest,
) (resp *gen.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendDescribeWorkflowExecutionScope, request)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
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
	domainID, err := wh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateExecutionAndEmitMetrics(request.Execution, scope); err != nil {
		return nil, err
	}

	response, err := wh.history.DescribeWorkflowExecution(ctx, &h.DescribeWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		Request:    request,
	})

	if err != nil {
		return nil, wh.error(err, scope)
	}

	return response, nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes. If includeTaskListStatus field is true,
// it will also return status of tasklist's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (wh *WorkflowHandler) DescribeTaskList(
	ctx context.Context,
	request *gen.DescribeTaskListRequest,
) (resp *gen.DescribeTaskListResponse, retError error) {
	defer log.CapturePanic(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfileWithDomain(metrics.FrontendDescribeTaskListScope, request)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
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
	domainID, err := wh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, wh.error(err, scope)
	}

	if err := wh.validateTaskList(request.TaskList, scope); err != nil {
		return nil, err
	}

	if err := wh.validateTaskListType(request.TaskListType, scope); err != nil {
		return nil, err
	}

	var response *gen.DescribeTaskListResponse
	op := func() error {
		var err error
		response, err = wh.matching.DescribeTaskList(ctx, &m.DescribeTaskListRequest{
			DomainUUID:  common.StringPtr(domainID),
			DescRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	return response, nil
}

func (wh *WorkflowHandler) getHistory(
	scope metrics.Scope,
	domainID string,
	execution gen.WorkflowExecution,
	firstEventID, nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *gen.TransientDecisionInfo,
	branchToken []byte,
) (*gen.History, []byte, error) {

	historyEvents := []*gen.HistoryEvent{}
	var size int

	shardID := common.WorkflowIDToHistoryShard(*execution.WorkflowId, wh.config.NumHistoryShards)
	var err error
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageV2Events(wh.historyV2Mgr, &persistence.ReadHistoryBranchRequest{
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

	if len(nextPageToken) == 0 && transientDecision != nil {
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &gen.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) getLoggerForTask(taskToken []byte) log.Logger {
	logger := wh.Service.GetLogger()
	task, err := wh.tokenSerializer.Deserialize(taskToken)
	if err == nil {
		logger = logger.WithTags(tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.WorkflowScheduleID(task.ScheduleID))
	}
	return logger
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandler) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	wh.startWG.Wait()

	metricsScope := wh.metricsClient.Scope(scope).Tagged(metrics.DomainUnknownTag())
	// timer should be emitted with the all tag
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

// startRequestProfileWithDomain initiates recording of request metrics and returns a domain tagged scope
func (wh *WorkflowHandler) startRequestProfileWithDomain(scope int, d domainGetter) (metrics.Scope, metrics.Stopwatch) {
	wh.startWG.Wait()

	var metricsScope metrics.Scope
	if d != nil {
		metricsScope = wh.metricsClient.Scope(scope).Tagged(metrics.DomainTag(d.GetDomain()))
	} else {
		metricsScope = wh.metricsClient.Scope(scope).Tagged(metrics.DomainUnknownTag())
	}
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

// getDefaultScope returns a default scope to use for request metrics
func (wh *WorkflowHandler) getDefaultScope(scope int) metrics.Scope {
	return wh.metricsClient.Scope(scope).Tagged(metrics.DomainUnknownTag())
}

func (wh *WorkflowHandler) error(err error, scope metrics.Scope) error {
	switch err := err.(type) {
	case *gen.InternalServiceError:
		wh.Service.GetLogger().Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		// NOTE: For internal error, we won't return thrift error from cadence-frontend.
		// Because in uber internal metrics, thrift errors are counted as user errors
		return fmt.Errorf("cadence internal error, msg: %v", err.Message)
	case *gen.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.DomainNotActiveError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return err
	case *gen.EntityNotExistsError:
		scope.IncCounter(metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		scope.IncCounter(metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		scope.IncCounter(metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	case *gen.CancellationAlreadyRequestedError:
		scope.IncCounter(metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return err
	case *gen.QueryFailedError:
		scope.IncCounter(metrics.CadenceErrQueryFailedCounter)
		return err
	case *gen.LimitExceededError:
		scope.IncCounter(metrics.CadenceErrLimitExceededCounter)
		return err
	case *gen.ClientVersionNotSupportedError:
		scope.IncCounter(metrics.CadenceErrClientVersionNotSupportedCounter)
		return err
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			return err
		}
	}

	wh.Service.GetLogger().Error("Uncategorized error",
		tag.Error(err))
	scope.IncCounter(metrics.CadenceFailures)
	return fmt.Errorf("cadence internal uncategorized error, msg: %v", err.Error())
}

func (wh *WorkflowHandler) validateTaskListType(t *gen.TaskListType, scope metrics.Scope) error {
	if t == nil {
		return wh.error(errTaskListTypeNotSet, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateTaskList(t *gen.TaskList, scope metrics.Scope) error {
	if t == nil || t.Name == nil || t.GetName() == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	if len(t.GetName()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errTaskListTooLong, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateExecutionAndEmitMetrics(w *gen.WorkflowExecution, scope metrics.Scope) error {
	err := validateExecution(w)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func validateExecution(w *gen.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.WorkflowId == nil || w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	matchingResp *m.PollForDecisionTaskResponse,
	branchToken []byte,
) (*gen.PollForDecisionTaskResponse, error) {

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
		domain, dErr := wh.domainCache.GetDomainByID(domainID)
		if dErr != nil {
			return nil, dErr
		}
		scope = scope.Tagged(metrics.DomainTag(domain.GetInfo().Name))
		history, persistenceToken, err = wh.getHistory(
			scope,
			domainID,
			*matchingResp.WorkflowExecution,
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(domain.GetInfo().Name)),
			nil,
			matchingResp.DecisionInfo,
			branchToken,
		)
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
				BranchToken:       branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &gen.PollForDecisionTaskResponse{
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
	}

	return resp, nil
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

func isFailoverRequest(updateRequest *gen.UpdateDomainRequest) bool {
	return updateRequest.ReplicationConfiguration != nil && updateRequest.ReplicationConfiguration.ActiveClusterName != nil
}

func (wh *WorkflowHandler) historyArchived(ctx context.Context, request *gen.GetWorkflowExecutionHistoryRequest, domainID string) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunId() == "" {
		return false
	}
	getMutableStateRequest := &h.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  request.Execution,
	}
	_, err := wh.history.GetMutableState(ctx, getMutableStateRequest)
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

func (wh *WorkflowHandler) getArchivedHistory(
	ctx context.Context,
	request *gen.GetWorkflowExecutionHistoryRequest,
	domainID string,
	scope metrics.Scope,
) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	entry, err := wh.domainCache.GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	URIString := entry.GetConfig().HistoryArchivalURI
	if URIString == "" {
		return nil, wh.error(errHistoryHasPassedRetentionPeriod, scope)
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

	history := &shared.History{}
	for _, batch := range resp.HistoryBatches {
		history.Events = append(history.Events, batch.Events...)
	}
	return &gen.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      common.BoolPtr(true),
	}, nil
}

func (wh *WorkflowHandler) convertIndexedKeyToThrift(keys map[string]interface{}) map[string]gen.IndexedValueType {
	converted := make(map[string]gen.IndexedValueType)
	for k, v := range keys {
		converted[k] = common.ConvertIndexedValueTypeToThriftType(v, wh.GetLogger())
	}
	return converted
}

func (wh *WorkflowHandler) isListRequestPageSizeTooLarge(pageSize int32, domain string) bool {
	return wh.config.EnableReadVisibilityFromES(domain) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
}

func (wh *WorkflowHandler) allow(d domainGetter) bool {
	domain := ""
	if d != nil {
		domain = d.GetDomain()
	}
	return wh.rateLimiter.Allow(quotas.Info{Domain: domain})
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (wh *WorkflowHandler) GetReplicationMessages(
	ctx context.Context,
	request *replicator.GetReplicationMessagesRequest,
) (resp *replicator.GetReplicationMessagesResponse, err error) {
	defer log.CapturePanic(wh.GetLogger(), &err)

	scope, sw := wh.startRequestProfile(metrics.FrontendGetReplicationMessagesScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	resp, err = wh.history.GetReplicationMessages(ctx, request)
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return resp, nil
}

// GetDomainReplicationMessages returns new domain replication tasks since last retrieved task ID.
func (wh *WorkflowHandler) GetDomainReplicationMessages(
	ctx context.Context,
	request *replicator.GetDomainReplicationMessagesRequest,
) (resp *replicator.GetDomainReplicationMessagesResponse, err error) {
	defer log.CapturePanic(wh.GetLogger(), &err)

	scope, sw := wh.startRequestProfile(metrics.FrontendGetDomainReplicationMessagesScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if request == nil {
		return nil, wh.error(errRequestNotSet, scope)
	}

	if wh.domainReplicationQueue == nil {
		return nil, wh.error(errors.New("domain replication queue not enabled for cluster"), scope)
	}

	lastMessageID := defaultLastMessageID
	if request.IsSetLastRetrivedMessageId() {
		lastMessageID = int(request.GetLastRetrivedMessageId())
	}

	if lastMessageID == defaultLastMessageID {
		clusterAckLevels, err := wh.domainReplicationQueue.GetAckLevels()
		if err == nil {
			if ackLevel, ok := clusterAckLevels[request.GetClusterName()]; ok {
				lastMessageID = ackLevel
			}
		}
	}

	replicationTasks, lastMessageID, err := wh.domainReplicationQueue.GetReplicationMessages(
		lastMessageID, getDomainReplicationMessageBatchSize)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	lastProcessedMessageID := defaultLastMessageID
	if request.IsSetLastProcessedMessageId() {
		lastProcessedMessageID = int(request.GetLastProcessedMessageId())
	}

	if lastProcessedMessageID != defaultLastMessageID {
		err := wh.domainReplicationQueue.UpdateAckLevel(lastProcessedMessageID, request.GetClusterName())
		if err != nil {
			wh.GetLogger().Warn("Failed to update domain replication queue ack level.",
				tag.TaskID(int64(lastProcessedMessageID)),
				tag.ClusterName(request.GetClusterName()))
		}
	}

	return &replicator.GetDomainReplicationMessagesResponse{
		Messages: &replicator.ReplicationMessages{
			ReplicationTasks:      replicationTasks,
			LastRetrivedMessageId: common.Int64Ptr(int64(lastMessageID)),
		},
	}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (wh *WorkflowHandler) ReapplyEvents(
	ctx context.Context,
	request *shared.ReapplyEventsRequest,
) (err error) {
	defer log.CapturePanic(wh.GetLogger(), &err)

	scope, sw := wh.startRequestProfile(metrics.FrontendReapplyEventsScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return wh.error(err, scope)
	}

	if request == nil {
		return wh.error(errRequestNotSet, scope)
	}
	if request.DomainName == nil || request.GetDomainName() == "" {
		return wh.error(errDomainNotSet, scope)
	}
	if request.WorkflowExecution == nil {
		return wh.error(errExecutionNotSet, scope)
	}
	if request.GetWorkflowExecution().GetWorkflowId() == "" {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	if request.GetEvents() == nil {
		return wh.error(errWorkflowIDNotSet, scope)
	}
	domainEntry, err := wh.domainCache.GetDomain(request.GetDomainName())
	if err != nil {
		return wh.error(err, scope)
	}

	err = wh.history.ReapplyEvents(ctx, &h.ReapplyEventsRequest{
		DomainUUID: common.StringPtr(domainEntry.GetInfo().ID),
		Request:    request,
	})
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func (wh *WorkflowHandler) checkPermission(
	securityToken *string,
) error {

	if wh.config.EnableAdminProtection() {
		if securityToken == nil {
			return errNoPermission
		}
		requiredToken := wh.config.AdminOperationToken()
		if *securityToken != requiredToken {
			return errNoPermission
		}
	}
	return nil
}

type domainWrapper struct {
	domain string
}

func (d domainWrapper) GetDomain() string {
	return d.domain
}
