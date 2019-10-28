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
	"fmt"
	"sync"

	"github.com/temporalio/temporal/.gen/go/health"
	"github.com/temporalio/temporal/.gen/go/health/metaserver"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common/log/tag"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/elasticsearch/validator"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/service"
	"github.com/temporalio/temporal/tpb"
)

var _ tpb.WorkflowServiceYARPCServer = (*WorkflowHandlerGRPC)(nil)

type (
	// WorkflowHandlerGRPC - gRPC handler interface for workflow service
	WorkflowHandlerGRPC struct {
		domainCache               cache.DomainCache
		metadataMgr               persistence.MetadataManager
		historyV2Mgr              persistence.HistoryManager
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
)

var (
	errDomainNotSetGRPC                               = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Domain not set on request.")
	errTaskTokenNotSetGRPC                            = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Task token not set on request.")
	errInvalidTaskTokenGRPC                           = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Invalid TaskToken.")
	errInvalidRequestTypeGRPC                         = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Invalid request type.")
	errTaskListNotSetGRPC                             = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "TaskList is not set on request.")
	errTaskListTypeNotSetGRPC                         = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "TaskListType is not set on request.")
	errExecutionNotSetGRPC                            = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Execution is not set on request.")
	errWorkflowIDNotSetGRPC                           = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "WorkflowId is not set on request.")
	errRunIDNotSetGRPC                                = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "RunId is not set on request.")
	errActivityIDNotSetGRPC                           = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "ActivityID is not set on request.")
	errInvalidRunIDGRPC                               = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Invalid RunId.")
	errInvalidNextPageTokenGRPC                       = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Invalid NextPageToken.")
	errNextPageTokenRunIDMismatchGRPC                 = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "RunID in the request does not match the NextPageToken.")
	errQueryNotSetGRPC                                = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "WorkflowQuery is not set on request.")
	errQueryTypeNotSetGRPC                            = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "QueryType is not set on request.")
	errRequestNotSetGRPC                              = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Request is nil.")
	errNoPermissionGRPC                               = protobuf.NewError(yarpcerrors.CodePermissionDenied, "No permission to do this operation.")
	errRequestIDNotSetGRPC                            = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "RequestId is not set on request.")
	errWorkflowTypeNotSetGRPC                         = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "WorkflowType is not set on request.")
	errInvalidExecutionStartToCloseTimeoutSecondsGRPC = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "A valid ExecutionStartToCloseTimeoutSeconds is not set on request.")
	errInvalidTaskStartToCloseTimeoutSecondsGRPC      = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "A valid TaskStartToCloseTimeoutSeconds is not set on request.")
	errClientVersionNotSetGRPC                        = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Client version is not set on request.")

	// err for archival
	errHistoryHasPassedRetentionPeriodGRPC = protobuf.NewError(yarpcerrors.CodeDeadlineExceeded, "Requested workflow history has passed retention period.")
	// the following errors represents bad user input
	errURIUpdateGRPC = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Cannot update existing archival URI")

	// err for string too long
	errDomainTooLongGRPC       = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Domain length exceeds limit.")
	errWorkflowTypeTooLongGRPC = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "WorkflowType length exceeds limit.")
	errWorkflowIDTooLongGRPC   = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "WorkflowID length exceeds limit.")
	errSignalNameTooLongGRPC   = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "SignalName length exceeds limit.")
	errTaskListTooLongGRPC     = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "TaskList length exceeds limit.")
	errRequestIDTooLongGRPC    = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "RequestID length exceeds limit.")
	errIdentityTooLongGRPC     = protobuf.NewError(yarpcerrors.CodeInvalidArgument, "Identity length exceeds limit.")
)

// NewWorkflowHandlerGRPC creates a thrift handler for the cadence service
func NewWorkflowHandlerGRPC(
	sVice service.Service,
	config *Config,
	metadataMgr persistence.MetadataManager,
	historyV2Mgr persistence.HistoryManager,
	visibilityMgr persistence.VisibilityManager,
	replicationMessageSink messaging.Producer,
	domainReplicationQueue persistence.DomainReplicationQueue,
	domainCache cache.DomainCache,
) *WorkflowHandlerGRPC {
	handler := &WorkflowHandlerGRPC{
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
func (wh *WorkflowHandlerGRPC) RegisterHandler() {
	wh.Service.GetDispatcher().Register(tpb.BuildWorkflowServiceYARPCProcedures(wh))
	wh.Service.GetDispatcher().Register(metaserver.New(wh))
}

// Start starts the handler
func (wh *WorkflowHandlerGRPC) Start() error {
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
func (wh *WorkflowHandlerGRPC) Stop() {
	wh.domainReplicationQueue.Close()
	wh.domainCache.Stop()
	wh.metadataMgr.Close()
	wh.visibilityMgr.Close()
	wh.Service.Stop()
}

// Health is for health check
func (wh *WorkflowHandlerGRPC) Health(ctx context.Context) (*health.HealthStatus, error) {
	wh.startWG.Wait()
	wh.GetLogger().Debug("Frontend health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("frontend good")}
	return hs, nil
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, registerRequest *tpb.RegisterDomainRequest) (_ *tpb.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.GetLogger(), &retError)

	scope, sw := wh.startRequestProfile(metrics.FrontendRegisterDomainScope)
	defer sw.Stop()

	if err := wh.versionChecker.checkClientVersion(ctx); err != nil {
		return nil, wh.error(err, scope)
	}

	if registerRequest == nil {
		return nil, errRequestNotSetGRPC
	}

	if err := wh.checkPermission(registerRequest.SecurityToken); err != nil {
		return nil, err
	}

	if registerRequest.GetName() == "" {
		return nil, errDomainNotSetGRPC
	}

	err := wh.domainHandler.RegisterDomain(ctx, transformThrift2GRPC(registerRequest))
	if err != nil {
		return nil, wh.error(err, scope)
	}
	return nil, nil
}

func transformThrift2GRPC(registerRequest *tpb.RegisterDomainRequest) *shared.RegisterDomainRequest {
	historyArchivalStatus := shared.ArchivalStatus(int32(registerRequest.HistoryArchivalStatus))
	visibilityArchivalStatus := shared.ArchivalStatus(int32(registerRequest.VisibilityArchivalStatus))

	var clusters []*shared.ClusterReplicationConfiguration
	for _, cluster := range registerRequest.Clusters {
		clusters = append(clusters, &shared.ClusterReplicationConfiguration{ClusterName: &cluster.ClusterName})
	}

	return &shared.RegisterDomainRequest{
		Name:                                   &registerRequest.Name,
		Description:                            &registerRequest.Description,
		OwnerEmail:                             &registerRequest.OwnerEmail,
		WorkflowExecutionRetentionPeriodInDays: &registerRequest.WorkflowExecutionRetentionPeriodInDays,
		EmitMetric:                             &registerRequest.EmitMetric,
		Clusters:                               clusters,
		ActiveClusterName:                      &registerRequest.ActiveClusterName,
		Data:                                   registerRequest.Data,
		SecurityToken:                          &registerRequest.SecurityToken,
		IsGlobalDomain:                         &registerRequest.IsGlobalDomain,
		HistoryArchivalStatus:                  &historyArchivalStatus,
		HistoryArchivalURI:                     &registerRequest.HistoryArchivalURI,
		VisibilityArchivalStatus:               &visibilityArchivalStatus,
		VisibilityArchivalURI:                  &registerRequest.VisibilityArchivalURI,
	}
}

func (wh *WorkflowHandlerGRPC) error(err error, scope metrics.Scope) error {
	// todo: return status
	switch err := err.(type) {
	case *shared.InternalServiceError:
		wh.Service.GetLogger().Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		// NOTE: For internal error, we won't return thrift error from cadence-frontend.
		// Because in uber internal metrics, thrift errors are counted as user errors
		//return fmt.Errorf("cadence internal error, msg: %v", err.Message)
		return protobuf.NewError(yarpcerrors.CodeInternal, err.Message)
	case *shared.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return protobuf.NewError(yarpcerrors.CodeInvalidArgument, err.Message)
	case *shared.DomainNotActiveError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return protobuf.NewError(yarpcerrors.CodeInvalidArgument, err.Message, protobuf.WithErrorDetails(
			&tpb.DomainNotActiveFailure{
				Message:        err.Message,
				DomainName:     err.DomainName,
				CurrentCluster: err.CurrentCluster,
				ActiveCluster:  err.ActiveCluster,
			},
		))
	case *shared.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return protobuf.NewError(yarpcerrors.CodeResourceExhausted, err.Message)
	case *shared.EntityNotExistsError:
		scope.IncCounter(metrics.CadenceErrEntityNotExistsCounter)
		return protobuf.NewError(yarpcerrors.CodeNotFound, err.Message)
	case *shared.WorkflowExecutionAlreadyStartedError:
		scope.IncCounter(metrics.CadenceErrExecutionAlreadyStartedCounter)
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, *err.Message, protobuf.WithErrorDetails(
			&tpb.WorkflowExecutionAlreadyStartedFailure{
				Message:        *err.Message,
				StartRequestId: *err.StartRequestId,
				RunId:          *err.RunId,
			},
		))
	case *shared.DomainAlreadyExistsError:
		scope.IncCounter(metrics.CadenceErrDomainAlreadyExistsCounter)
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, err.Message)
	case *shared.CancellationAlreadyRequestedError:
		scope.IncCounter(metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, err.Message)
	case *shared.QueryFailedError:
		scope.IncCounter(metrics.CadenceErrQueryFailedCounter)
		return protobuf.NewError(yarpcerrors.CodeInternal, err.Message)
	case *shared.LimitExceededError:
		scope.IncCounter(metrics.CadenceErrLimitExceededCounter)
		return protobuf.NewError(yarpcerrors.CodeResourceExhausted, err.Message)
	case *shared.ClientVersionNotSupportedError:
		scope.IncCounter(metrics.CadenceErrClientVersionNotSupportedCounter)
		return protobuf.NewError(yarpcerrors.CodeFailedPrecondition, "Client version is not supported.", protobuf.WithErrorDetails(
			&tpb.ClientVersionNotSupportedFailure{
				FeatureVersion: err.FeatureVersion, ClientImpl: err.ClientImpl, SupportedVersions: err.SupportedVersions,
			},
		))
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			return protobuf.NewError(yarpcerrors.CodeDeadlineExceeded, err.Message())
		}
	}

	wh.Service.GetLogger().Error("Uncategorized error", tag.Error(err))
	scope.IncCounter(metrics.CadenceFailures)
	return protobuf.NewError(yarpcerrors.CodeInternal, fmt.Sprintf("cadence internal uncategorized error, msg: %v", err.Error()))
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandlerGRPC) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	wh.startWG.Wait()

	metricsScope := wh.metricsClient.Scope(scope).Tagged(metrics.DomainUnknownTag())
	// timer should be emitted with the all tag
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

func (wh *WorkflowHandlerGRPC) checkPermission(
	securityToken string,
) error {

	if wh.config.EnableAdminProtection() {
		if securityToken == "" {
			return errNoPermissionGRPC
		}
		requiredToken := wh.config.AdminOperationToken()
		if securityToken != requiredToken {
			return errNoPermissionGRPC
		}
	}
	return nil
}
