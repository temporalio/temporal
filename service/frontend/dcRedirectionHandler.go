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
	"time"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
)

var _ workflowserviceserver.Interface = (*DCRedirectionHandlerImpl)(nil)

type (
	clientBeanProvider func() client.Bean

	// DCRedirectionHandlerImpl is simple wrapper over frontend service, doing redirection based on policy
	DCRedirectionHandlerImpl struct {
		currentClusterName string
		timeSource         clock.TimeSource
		domainCache        cache.DomainCache
		metricsClient      metrics.Client
		config             *Config
		redirectionPolicy  DCRedirectionPolicy
		tokenSerializer    common.TaskTokenSerializer
		service            service.Service
		frontendHandler    workflowserviceserver.Interface
		clientBeanProvider clientBeanProvider

		startFn func() error
		stopFn  func()
	}
)

// NewDCRedirectionHandler creates a thrift handler for the cadence service, frontend
func NewDCRedirectionHandler(wfHandler *WorkflowHandler, policy config.DCRedirectionPolicy) *DCRedirectionHandlerImpl {
	dcRedirectionPolicy := RedirectionPolicyGenerator(
		wfHandler.GetClusterMetadata(),
		wfHandler.config,
		wfHandler.domainCache,
		policy,
	)

	return &DCRedirectionHandlerImpl{
		currentClusterName: wfHandler.GetClusterMetadata().GetCurrentClusterName(),
		timeSource:         clock.NewRealTimeSource(),
		domainCache:        wfHandler.domainCache,
		metricsClient:      wfHandler.metricsClient,
		config:             wfHandler.config,
		redirectionPolicy:  dcRedirectionPolicy,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		service:            wfHandler.Service,
		frontendHandler:    wfHandler,
		clientBeanProvider: func() client.Bean { return wfHandler.Service.GetClientBean() },
		startFn:            func() error { return wfHandler.Start() },
		stopFn:             func() { wfHandler.Stop() },
	}
}

// RegisterHandler register this handler, must be called before Start()
func (handler *DCRedirectionHandlerImpl) RegisterHandler() {
	handler.service.GetDispatcher().Register(workflowserviceserver.New(handler))
	handler.service.GetDispatcher().Register(metaserver.New(handler))
}

// Start starts the handler
func (handler *DCRedirectionHandlerImpl) Start() error {
	return handler.startFn()
}

// Stop stops the handler
func (handler *DCRedirectionHandlerImpl) Stop() {
	handler.stopFn()
}

// Health is for health check
func (handler *DCRedirectionHandlerImpl) Health(ctx context.Context) (*health.HealthStatus, error) {
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("dc redirection good")}
	return hs, nil
}

// Domain APIs, domain APIs does not require redirection

// DeprecateDomain API call
func (handler *DCRedirectionHandlerImpl) DeprecateDomain(
	ctx context.Context,
	request *shared.DeprecateDomainRequest,
) (retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDeprecateDomainScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.DeprecateDomain(ctx, request)
}

// DescribeDomain API call
func (handler *DCRedirectionHandlerImpl) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
) (resp *shared.DescribeDomainResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeDomainScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.DescribeDomain(ctx, request)
}

// ListDomains API call
func (handler *DCRedirectionHandlerImpl) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
) (resp *shared.ListDomainsResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListDomainsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.ListDomains(ctx, request)
}

// RegisterDomain API call
func (handler *DCRedirectionHandlerImpl) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
) (retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRegisterDomainScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.RegisterDomain(ctx, request)
}

// UpdateDomain API call
func (handler *DCRedirectionHandlerImpl) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
) (resp *shared.UpdateDomainResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionUpdateDomainScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.UpdateDomain(ctx, request)
}

// Other APIs

// DescribeTaskList API call
func (handler *DCRedirectionHandlerImpl) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
) (resp *shared.DescribeTaskListResponse, retError error) {

	var apiName = "DescribeTaskList"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeTaskListScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeTaskList(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.DescribeTaskList(ctx, request)
		}
		return err
	})

	return resp, err
}

// DescribeWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
) (resp *shared.DescribeWorkflowExecutionResponse, retError error) {

	var apiName = "DescribeWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.DescribeWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetWorkflowExecutionHistory API call
func (handler *DCRedirectionHandlerImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
) (resp *shared.GetWorkflowExecutionHistoryResponse, retError error) {

	var apiName = "GetWorkflowExecutionHistory"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetWorkflowExecutionHistoryScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.GetWorkflowExecutionHistory(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListArchivedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListArchivedWorkflowExecutionsRequest,
) (resp *shared.ListArchivedWorkflowExecutionsResponse, retError error) {

	var apiName = "ListArchivedWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListArchviedWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListArchivedWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListArchivedWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListClosedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
) (resp *shared.ListClosedWorkflowExecutionsResponse, retError error) {

	var apiName = "ListClosedWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListClosedWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListClosedWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListOpenWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
) (resp *shared.ListOpenWorkflowExecutionsResponse, retError error) {

	var apiName = "ListOpenWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListOpenWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListOpenWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
) (resp *shared.ListWorkflowExecutionsResponse, retError error) {

	var apiName = "ListWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ScanWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
) (resp *shared.ListWorkflowExecutionsResponse, retError error) {

	var apiName = "ScanWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionScanWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()
	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ScanWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ScanWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// CountWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *shared.CountWorkflowExecutionsRequest,
) (resp *shared.CountWorkflowExecutionsResponse, retError error) {

	var apiName = "CountWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionCountWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.CountWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.CountWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetSearchAttributes API call
func (handler *DCRedirectionHandlerImpl) GetSearchAttributes(
	ctx context.Context,
) (resp *shared.GetSearchAttributesResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetSearchAttributesScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.GetSearchAttributes(ctx)
}

// PollForActivityTask API call
func (handler *DCRedirectionHandlerImpl) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
) (resp *shared.PollForActivityTaskResponse, retError error) {

	var apiName = "PollForActivityTask"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollForActivityTaskScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForActivityTask(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.PollForActivityTask(ctx, request)
		}
		return err
	})

	return resp, err
}

// PollForDecisionTask API call
func (handler *DCRedirectionHandlerImpl) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
) (resp *shared.PollForDecisionTaskResponse, retError error) {

	var apiName = "PollForDecisionTask"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollForDecisionTaskScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForDecisionTask(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.PollForDecisionTask(ctx, request)
		}
		return err
	})

	return resp, err
}

// QueryWorkflow API call
func (handler *DCRedirectionHandlerImpl) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
) (resp *shared.QueryWorkflowResponse, retError error) {

	var apiName = "QueryWorkflow"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionQueryWorkflowScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.QueryWorkflow(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.QueryWorkflow(ctx, request)
		}
		return err
	})

	return resp, err
}

// RecordActivityTaskHeartbeat API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
) (resp *shared.RecordActivityTaskHeartbeatResponse, retError error) {

	var apiName = "RecordActivityTaskHeartbeat"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRecordActivityTaskHeartbeatScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RecordActivityTaskHeartbeat(ctx, request)
		}
		return err
	})

	return resp, err
}

// RecordActivityTaskHeartbeatByID API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
) (resp *shared.RecordActivityTaskHeartbeatResponse, retError error) {

	var apiName = "RecordActivityTaskHeartbeatByID"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRecordActivityTaskHeartbeatByIDScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeatByID(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RecordActivityTaskHeartbeatByID(ctx, request)
		}
		return err
	})

	return resp, err
}

// RequestCancelWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
) (retError error) {

	var apiName = "RequestCancelWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRequestCancelWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RequestCancelWorkflowExecution(ctx, request)
		}
		return err
	})

	return err
}

// ResetStickyTaskList API call
func (handler *DCRedirectionHandlerImpl) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
) (resp *shared.ResetStickyTaskListResponse, retError error) {

	var apiName = "ResetStickyTaskList"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionResetStickyTaskListScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetStickyTaskList(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ResetStickyTaskList(ctx, request)
		}
		return err
	})

	return resp, err
}

// ResetWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
) (resp *shared.ResetWorkflowExecutionResponse, retError error) {

	var apiName = "ResetWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionResetWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ResetWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskCanceled API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledRequest,
) (retError error) {

	var apiName = "RespondActivityTaskCanceled"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCanceledScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskCanceled(ctx, request)
		}
		return err
	})

	return err
}

// RespondActivityTaskCanceledByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
) (retError error) {

	var apiName = "RespondActivityTaskCanceledByID"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCanceledByIDScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCanceledByID(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskCanceledByID(ctx, request)
		}
		return err
	})

	return err
}

// RespondActivityTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
) (retError error) {

	var apiName = "RespondActivityTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskCompleted(ctx, request)
		}
		return err
	})

	return err
}

// RespondActivityTaskCompletedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
) (retError error) {

	var apiName = "RespondActivityTaskCompletedByID"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCompletedByIDScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCompletedByID(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskCompletedByID(ctx, request)
		}
		return err
	})

	return err
}

// RespondActivityTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
) (retError error) {

	var apiName = "RespondActivityTaskFailed"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskFailedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskFailed(ctx, request)
		}
		return err
	})

	return err
}

// RespondActivityTaskFailedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
) (retError error) {

	var apiName = "RespondActivityTaskFailedByID"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskFailedByIDScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskFailedByID(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondActivityTaskFailedByID(ctx, request)
		}
		return err
	})

	return err
}

// RespondDecisionTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
) (resp *shared.RespondDecisionTaskCompletedResponse, retError error) {

	var apiName = "RespondDecisionTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondDecisionTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondDecisionTaskCompleted(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondDecisionTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
) (retError error) {

	var apiName = "RespondDecisionTaskFailed"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondDecisionTaskFailedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondDecisionTaskFailed(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondDecisionTaskFailed(ctx, request)
		}
		return err
	})

	return err
}

// RespondQueryTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
) (retError error) {

	var apiName = "RespondQueryTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondQueryTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(ctx, token.DomainID, apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.RespondQueryTaskCompleted(ctx, request)
		}
		return err
	})

	return err
}

// SignalWithStartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
) (resp *shared.StartWorkflowExecutionResponse, retError error) {

	var apiName = "SignalWithStartWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionSignalWithStartWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.SignalWithStartWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// SignalWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWorkflowExecutionRequest,
) (retError error) {

	var apiName = "SignalWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionSignalWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.SignalWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.SignalWorkflowExecution(ctx, request)
		}
		return err
	})
	return err
}

// StartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
) (resp *shared.StartWorkflowExecutionResponse, retError error) {

	var apiName = "StartWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionStartWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.StartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.StartWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// TerminateWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *shared.TerminateWorkflowExecutionRequest,
) (retError error) {

	var apiName = "TerminateWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionTerminateWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithDomainNameRedirect(ctx, request.GetDomain(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBeanProvider().GetRemoteFrontendClient(targetDC)
			err = remoteClient.TerminateWorkflowExecution(ctx, request)
		}
		return err
	})

	return err
}

// GetReplicationMessages API call
func (handler *DCRedirectionHandlerImpl) GetReplicationMessages(
	ctx context.Context,
	request *replicator.GetReplicationMessagesRequest,
) (*replicator.GetReplicationMessagesResponse, error) {
	return handler.frontendHandler.GetReplicationMessages(ctx, request)
}

// GetDomainReplicationMessages API call
func (handler *DCRedirectionHandlerImpl) GetDomainReplicationMessages(
	ctx context.Context,
	request *replicator.GetDomainReplicationMessagesRequest,
) (*replicator.GetDomainReplicationMessagesResponse, error) {
	return handler.frontendHandler.GetDomainReplicationMessages(ctx, request)
}

// ReapplyEvents API call
func (handler *DCRedirectionHandlerImpl) ReapplyEvents(
	ctx context.Context,
	request *shared.ReapplyEventsRequest,
) error {
	return handler.frontendHandler.ReapplyEvents(ctx, request)
}

func (handler *DCRedirectionHandlerImpl) beforeCall(
	scope int,
) (metrics.Scope, time.Time) {

	return handler.metricsClient.Scope(scope), handler.timeSource.Now()
}

func (handler *DCRedirectionHandlerImpl) afterCall(
	scope metrics.Scope,
	startTime time.Time,
	cluster string,
	retError *error,
) {

	log.CapturePanic(handler.service.GetLogger(), retError)

	scope = scope.Tagged(metrics.TargetClusterTag(cluster))
	scope.IncCounter(metrics.CadenceDcRedirectionClientRequests)
	scope.RecordTimer(metrics.CadenceDcRedirectionClientLatency, handler.timeSource.Now().Sub(startTime))
	if *retError != nil {
		scope.IncCounter(metrics.CadenceDcRedirectionClientFailures)
	}
}
