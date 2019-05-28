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

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
)

type (
	// DCRedirectionHandlerImpl is simple wrapper over frontend service, doing redirection based on policy
	DCRedirectionHandlerImpl struct {
		currentClusterName string
		domainCache        cache.DomainCache
		config             *Config
		redirectionPolicy  DCRedirectionPolicy
		tokenSerializer    common.TaskTokenSerializer
		service            service.Service
		frontendHandler    workflowserviceserver.Interface
		clientBean         client.Bean

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
		domainCache:        wfHandler.domainCache,
		config:             wfHandler.config,
		redirectionPolicy:  dcRedirectionPolicy,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		service:            wfHandler.Service,
		frontendHandler:    wfHandler,
		clientBean:         wfHandler.Service.GetClientBean(),
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
) error {

	return handler.frontendHandler.DeprecateDomain(ctx, request)
}

// DescribeDomain API call
func (handler *DCRedirectionHandlerImpl) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
) (*shared.DescribeDomainResponse, error) {

	return handler.frontendHandler.DescribeDomain(ctx, request)
}

// ListDomains API call
func (handler *DCRedirectionHandlerImpl) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
) (*shared.ListDomainsResponse, error) {

	return handler.frontendHandler.ListDomains(ctx, request)
}

// RegisterDomain API call
func (handler *DCRedirectionHandlerImpl) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
) error {

	return handler.frontendHandler.RegisterDomain(ctx, request)
}

// UpdateDomain API call
func (handler *DCRedirectionHandlerImpl) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
) (*shared.UpdateDomainResponse, error) {

	return handler.frontendHandler.UpdateDomain(ctx, request)
}

// Other APIs

// DescribeTaskList API call
func (handler *DCRedirectionHandlerImpl) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
) (*shared.DescribeTaskListResponse, error) {

	var apiName = "DescribeTaskList"
	var resp *shared.DescribeTaskListResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeTaskList(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.DescribeWorkflowExecutionResponse, error) {

	var apiName = "DescribeWorkflowExecution"
	var resp *shared.DescribeWorkflowExecutionResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	var apiName = "GetWorkflowExecutionHistory"
	var resp *shared.GetWorkflowExecutionHistoryResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.GetWorkflowExecutionHistory(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListClosedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	var apiName = "ListClosedWorkflowExecutions"
	var resp *shared.ListClosedWorkflowExecutionsResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	var apiName = "ListOpenWorkflowExecutions"
	var resp *shared.ListOpenWorkflowExecutionsResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.ListWorkflowExecutionsResponse, error) {

	var apiName = "ListWorkflowExecutions"
	var resp *shared.ListWorkflowExecutionsResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.ListWorkflowExecutionsResponse, error) {

	var apiName = "ScanWorkflowExecutions"
	var resp *shared.ListWorkflowExecutionsResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ScanWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.CountWorkflowExecutionsResponse, error) {

	var apiName = "CountWorkflowExecutions"
	var resp *shared.CountWorkflowExecutionsResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.CountWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.CountWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetSearchAttributes API call
func (handler *DCRedirectionHandlerImpl) GetSearchAttributes(
	ctx context.Context,
) (*shared.GetSearchAttributesResponse, error) {

	return handler.frontendHandler.GetSearchAttributes(ctx)
}

// PollForActivityTask API call
func (handler *DCRedirectionHandlerImpl) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
) (*shared.PollForActivityTaskResponse, error) {

	var apiName = "PollForActivityTask"
	var resp *shared.PollForActivityTaskResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForActivityTask(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.PollForDecisionTaskResponse, error) {

	var apiName = "PollForDecisionTask"
	var resp *shared.PollForDecisionTaskResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForDecisionTask(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.QueryWorkflowResponse, error) {

	var apiName = "QueryWorkflow"
	var resp *shared.QueryWorkflowResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.QueryWorkflow(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	var apiName = "RecordActivityTaskHeartbeat"
	var resp *shared.RecordActivityTaskHeartbeatResponse
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	var apiName = "RecordActivityTaskHeartbeatByID"
	var resp *shared.RecordActivityTaskHeartbeatResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeatByID(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RequestCancelWorkflowExecution"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.ResetStickyTaskListResponse, error) {

	var apiName = "ResetStickyTaskList"
	var resp *shared.ResetStickyTaskListResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetStickyTaskList(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.ResetWorkflowExecutionResponse, error) {

	var apiName = "ResetWorkflowExecution"
	var resp *shared.ResetWorkflowExecutionResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskCanceled"
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskCanceledByID"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCanceledByID(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskCompleted"
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskCompletedByID"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskCompletedByID(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskFailed"
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondActivityTaskFailedByID"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondActivityTaskFailedByID(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	var apiName = "RespondDecisionTaskCompleted"
	var resp *shared.RespondDecisionTaskCompletedResponse
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondDecisionTaskFailed"
	var err error

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondDecisionTaskFailed(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "RespondQueryTaskCompleted"
	var err error

	token, err := handler.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return err
	}

	err = handler.redirectionPolicy.WithDomainIDRedirect(token.DomainID, apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.StartWorkflowExecutionResponse, error) {

	var apiName = "SignalWithStartWorkflowExecution"
	var resp *shared.StartWorkflowExecutionResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "SignalWorkflowExecution"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.SignalWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) (*shared.StartWorkflowExecutionResponse, error) {

	var apiName = "StartWorkflowExecution"
	var resp *shared.StartWorkflowExecutionResponse
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.StartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
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
) error {

	var apiName = "TerminateWorkflowExecution"
	var err error

	err = handler.redirectionPolicy.WithDomainNameRedirect(request.GetDomain(), apiName, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.clientBean.GetRemoteFrontendClient(targetDC)
			err = remoteClient.TerminateWorkflowExecution(ctx, request)
		}
		return err
	})

	return err
}
