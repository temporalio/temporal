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
	"github.com/uber/cadence/common/service"
)

type (
	// DCRedirectionHandlerImpl is simple wrapper over fontend service, doing redirection based on policy
	DCRedirectionHandlerImpl struct {
		currentClusteName string
		redirectionPolicy DCRedirectionPolicy
		tokenSerializer   common.TaskTokenSerializer
		sevice            service.Service
		frontendHandler   *WorkflowHandler
	}
)

// NewDCRedirectionHandler creates a thrift handler for the cadence service, frontend and admin
func NewDCRedirectionHandler(currentClusteName string, redirectionPolicy DCRedirectionPolicy,
	sevice service.Service, frontendHandler *WorkflowHandler) *DCRedirectionHandlerImpl {
	return &DCRedirectionHandlerImpl{
		currentClusteName: currentClusteName,
		redirectionPolicy: redirectionPolicy,
		tokenSerializer:   common.NewJSONTaskTokenSerializer(),
		sevice:            sevice,
		frontendHandler:   frontendHandler,
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.DescribeTaskList(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).DescribeTaskList(ctx, request)
}

// DescribeWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *shared.DescribeWorkflowExecutionRequest,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).DescribeWorkflowExecution(ctx, request)
}

// GetWorkflowExecutionHistory API call
func (handler *DCRedirectionHandlerImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).GetWorkflowExecutionHistory(ctx, request)
}

// ListClosedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *shared.ListClosedWorkflowExecutionsRequest,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).ListClosedWorkflowExecutions(ctx, request)
}

// ListOpenWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).ListOpenWorkflowExecutions(ctx, request)
}

// PollForActivityTask API call
func (handler *DCRedirectionHandlerImpl) PollForActivityTask(
	ctx context.Context,
	request *shared.PollForActivityTaskRequest,
) (*shared.PollForActivityTaskResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.PollForActivityTask(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).PollForActivityTask(ctx, request)
}

// PollForDecisionTask API call
func (handler *DCRedirectionHandlerImpl) PollForDecisionTask(
	ctx context.Context,
	request *shared.PollForDecisionTaskRequest,
) (*shared.PollForDecisionTaskResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.PollForDecisionTask(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).PollForDecisionTask(ctx, request)
}

// QueryWorkflow API call
func (handler *DCRedirectionHandlerImpl) QueryWorkflow(
	ctx context.Context,
	request *shared.QueryWorkflowRequest,
) (*shared.QueryWorkflowResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.QueryWorkflow(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).QueryWorkflow(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RecordActivityTaskHeartbeat(ctx, request)
}

// RecordActivityTaskHeartbeatByID API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RecordActivityTaskHeartbeatByID(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RecordActivityTaskHeartbeatByID(ctx, request)
}

// RequestCancelWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RequestCancelWorkflowExecution(ctx, request)
}

// ResetStickyTaskList API call
func (handler *DCRedirectionHandlerImpl) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
) (*shared.ResetStickyTaskListResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.ResetStickyTaskList(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).ResetStickyTaskList(ctx, request)
}

// ResetWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *shared.ResetWorkflowExecutionRequest,
) (*shared.ResetWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.ResetWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).ResetWorkflowExecution(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCanceled(ctx, request)
}

// RespondActivityTaskCanceledByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskCanceledByID(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCanceledByID(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCompleted(ctx, request)
}

// RespondActivityTaskCompletedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskCompletedByID(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskCompletedByID(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskFailed(ctx, request)
}

// RespondActivityTaskFailedByID API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondActivityTaskFailedByID(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondActivityTaskFailedByID(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondDecisionTaskCompleted(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondDecisionTaskFailed(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondDecisionTaskFailed(ctx, request)
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

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByID(token.DomainID)
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).RespondQueryTaskCompleted(ctx, request)
}

// SignalWithStartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWithStartWorkflowExecutionRequest,
) (*shared.StartWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).SignalWithStartWorkflowExecution(ctx, request)
}

// SignalWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *shared.SignalWorkflowExecutionRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.SignalWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).SignalWorkflowExecution(ctx, request)
}

// StartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
) (*shared.StartWorkflowExecutionResponse, error) {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return nil, err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.StartWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).StartWorkflowExecution(ctx, request)
}

// TerminateWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *shared.TerminateWorkflowExecutionRequest,
) error {

	targetDC, err := handler.redirectionPolicy.GetTargetDatacenterByName(request.GetDomain())
	if err != nil {
		return err
	}

	if targetDC == handler.currentClusteName {
		return handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
	}

	return handler.sevice.GetClientBean().GetRemoteFrontendClient(targetDC).TerminateWorkflowExecution(ctx, request)
}
