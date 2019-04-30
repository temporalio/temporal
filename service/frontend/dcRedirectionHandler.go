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

	"github.com/uber/cadence/.gen/go/shared"
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
		frontendHandler    *WorkflowHandler
	}
)

// NewDCRedirectionHandler creates a thrift handler for the cadence service, frontend
func NewDCRedirectionHandler(wfHandler *WorkflowHandler, policy config.DCRedirectionPolicy) *DCRedirectionHandlerImpl {
	dcRedirectionPolicy := RedirectionPolicyGenerator(
		wfHandler.GetClusterMetadata(),
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
	}
}

// Start starts the handler
func (handler *DCRedirectionHandlerImpl) Start() error {
	return handler.frontendHandler.Start()
}

// Stop stops the handler
func (handler *DCRedirectionHandlerImpl) Stop() {
	handler.frontendHandler.Stop()
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

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.DescribeTaskList(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).DescribeTaskList(ctx, request)
}

// DescribeWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).DescribeWorkflowExecution(ctx, request)
}

// GetWorkflowExecutionHistory API call
func (handler *DCRedirectionHandlerImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).GetWorkflowExecutionHistory(ctx, request)
}

// ListClosedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ListClosedWorkflowExecutions(ctx, request)
}

// ListOpenWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ListOpenWorkflowExecutions(ctx, request)
}

// ListWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
) (*shared.ListWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ListWorkflowExecutions(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ListWorkflowExecutions(ctx, request)
}

// ScanWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
) (*shared.ListWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ScanWorkflowExecutions(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ScanWorkflowExecutions(ctx, request)
}

// PollForActivityTask API call
func (handler *DCRedirectionHandlerImpl) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
) (*shared.PollForActivityTaskResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.PollForActivityTask(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).PollForActivityTask(ctx, request)
}

// PollForDecisionTask API call
func (handler *DCRedirectionHandlerImpl) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
) (*shared.PollForDecisionTaskResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.PollForDecisionTask(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).PollForDecisionTask(ctx, request)
}

// QueryWorkflow API call
func (handler *DCRedirectionHandlerImpl) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
) (*shared.QueryWorkflowResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.QueryWorkflow(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).QueryWorkflow(ctx, request)
}

// RecordActivityTaskHeartbeat API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RecordActivityTaskHeartbeat(ctx, request)
}

// RecordActivityTaskHeartbeatByID API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RecordActivityTaskHeartbeatByID(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RecordActivityTaskHeartbeatByID(ctx, request)
}

// RequestCancelWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RequestCancelWorkflowExecution(ctx, request)
}

// ResetStickyTaskList API call
func (handler *DCRedirectionHandlerImpl) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
) (*shared.ResetStickyTaskListResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ResetStickyTaskList(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ResetStickyTaskList(ctx, request)
}

// ResetWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
) (*shared.ResetWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.ResetWorkflowExecution(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).ResetWorkflowExecution(ctx, request)
}

// RespondActivityTaskCanceled API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledRequest,
) error {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCanceled(ctx, request)
}

// RespondActivityTaskCanceledByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskCanceledByID(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCanceledByID(ctx, request)
}

// RespondActivityTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
) error {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCompleted(ctx, request)
}

// RespondActivityTaskCompletedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskCompletedByID(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCompletedByID(ctx, request)
}

// RespondActivityTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
) error {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskFailed(ctx, request)
}

// RespondActivityTaskFailedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondActivityTaskFailedByID(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskFailedByID(ctx, request)
}

// RespondDecisionTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
) (*shared.RespondDecisionTaskCompletedResponse, error) {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondDecisionTaskCompleted(ctx, request)
}

// RespondDecisionTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
) error {

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondDecisionTaskFailed(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondDecisionTaskFailed(ctx, request)
}

// RespondQueryTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
) error {

	token, err := handler.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return err
	}

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).RespondQueryTaskCompleted(ctx, request)
}

// SignalWithStartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
) (*shared.StartWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	var resp *shared.StartWorkflowExecutionResponse

	err = handler.withDomainNotActiveRedirect(request.GetDomain(), targetDC, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.service.GetClientBean().GetRemoteFrontendClient(targetDC)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	err = handler.withDomainNotActiveRedirect(request.GetDomain(), targetDC, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			err = handler.frontendHandler.SignalWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.service.GetClientBean().GetRemoteFrontendClient(targetDC)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	var resp *shared.StartWorkflowExecutionResponse

	err = handler.withDomainNotActiveRedirect(request.GetDomain(), targetDC, func(targetDC string) error {
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.StartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.service.GetClientBean().GetRemoteFrontendClient(targetDC)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDataCenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusterName {
		return handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
	}

	return handler.service.GetClientBean().GetRemoteFrontendClient(targetDC).TerminateWorkflowExecution(ctx, request)
}

func (handler *DCRedirectionHandlerImpl) isDomainNotActiveError(err error) (string, bool) {
	domainNotActiveErr, ok := err.(*shared.DomainNotActiveError)
	if !ok {
		return "", false
	}
	return domainNotActiveErr.ActiveCluster, true
}

func (handler *DCRedirectionHandlerImpl) enableDomainNotActiveAutoForwarding(domainName string) (bool, string, error) {
	domainEntry, err := handler.domainCache.GetDomain(domainName)
	if err != nil {
		return false, "", err
	}

	if !domainEntry.IsGlobalDomain() {
		return false, "", nil
	}

	if len(domainEntry.GetReplicationConfig().Clusters) == 1 {
		// do not do dc redirection if domain is only targeting at 1 dc (effectively local domain)
		return false, "", nil
	}

	if !handler.config.EnableDomainNotActiveAutoForwarding(domainEntry.GetInfo().Name) {
		// do not do dc redirection if domain is only targeting at 1 dc (effectively local domain)
		return false, "", nil
	}

	return true, domainEntry.GetReplicationConfig().ActiveClusterName, nil
}

func (handler *DCRedirectionHandlerImpl) withDomainNotActiveRedirect(domain string, targetDC string, call func(string) error) error {
	enableDomainNotActiveForwarding, activeCluster, err := handler.enableDomainNotActiveAutoForwarding(domain)
	if err != nil {
		return err
	}

	if enableDomainNotActiveForwarding {
		targetDC = activeCluster
	}

	err = call(targetDC)
	targetDC, ok := handler.isDomainNotActiveError(err)
	if !ok || !enableDomainNotActiveForwarding {
		return err
	}
	return call(targetDC)
}
