// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	"go.temporal.io/api/workflowservice/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
)

// TODO(vancexu): add metrics

// AccessControlledWorkflowHandler frontend handler wrapper for authentication and authorization
type AccessControlledWorkflowHandler struct {
	frontendHandler Handler
	authorizer      authorization.Authorizer
}

var _ Handler = (*AccessControlledWorkflowHandler)(nil)

// NewAccessControlledHandlerImpl creates frontend handler with authentication support
func NewAccessControlledHandlerImpl(wfHandler Handler, authorizer authorization.Authorizer) *AccessControlledWorkflowHandler {
	if authorizer == nil {
		authorizer = authorization.NewNopAuthorizer()
	}

	return &AccessControlledWorkflowHandler{
		frontendHandler: wfHandler,
		authorizer:      authorizer,
	}
}

// GetResource return resource
func (a *AccessControlledWorkflowHandler) GetResource() resource.Resource {
	return a.frontendHandler.GetResource()
}

// GetConfig return config
func (a *AccessControlledWorkflowHandler) GetConfig() *Config {
	return a.frontendHandler.GetConfig()
}

// UpdateHealthStatus sets the health status for this rpc handler.
// This health status will be used within the rpc health check handler
func (a *AccessControlledWorkflowHandler) UpdateHealthStatus(status HealthStatus) {
	a.frontendHandler.UpdateHealthStatus(status)
}

// https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func (a *AccessControlledWorkflowHandler) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return a.frontendHandler.Check(ctx, request)
}

func (a *AccessControlledWorkflowHandler) Watch(request *healthpb.HealthCheckRequest, server healthpb.Health_WatchServer) error {
	return a.frontendHandler.Watch(request, server)
}

// Start starts the handler
func (a *AccessControlledWorkflowHandler) Start() {
	a.frontendHandler.Start()
}

// Stop stops the handler
func (a *AccessControlledWorkflowHandler) Stop() {
	a.frontendHandler.Stop()
}

// CountWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendCountWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "CountWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.CountWorkflowExecutions(ctx, request)
}

// DeprecateNamespace API call
func (a *AccessControlledWorkflowHandler) DeprecateNamespace(
	ctx context.Context,
	request *workflowservice.DeprecateNamespaceRequest,
) (*workflowservice.DeprecateNamespaceResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendDeprecateNamespaceScope, request.GetName())

	attr := &authorization.Attributes{
		APIName:   "DeprecateNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DeprecateNamespace(ctx, request)
}

// DescribeNamespace API call
func (a *AccessControlledWorkflowHandler) DescribeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
) (*workflowservice.DescribeNamespaceResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendDescribeNamespaceScope, request.GetName())

	attr := &authorization.Attributes{
		APIName:   "DescribeNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeNamespace(ctx, request)
}

// DescribeTaskQueue API call
func (a *AccessControlledWorkflowHandler) DescribeTaskQueue(
	ctx context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
) (*workflowservice.DescribeTaskQueueResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendDescribeTaskQueueScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "DescribeTaskQueue",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeTaskQueue(ctx, request)
}

// DescribeWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendDescribeWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "DescribeWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeWorkflowExecution(ctx, request)
}

// GetSearchAttributes API call
func (a *AccessControlledWorkflowHandler) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
) (*workflowservice.GetSearchAttributesResponse, error) {
	return a.frontendHandler.GetSearchAttributes(ctx, request)
}

// GetWorkflowExecutionHistory API call
func (a *AccessControlledWorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendGetWorkflowExecutionHistoryScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "GetWorkflowExecutionHistory",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
}

// ListArchivedWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendListArchivedWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ListArchivedWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListArchivedWorkflowExecutions(ctx, request)
}

// ListClosedWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendListClosedWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ListClosedWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
}

// ListNamespaces API call
func (a *AccessControlledWorkflowHandler) ListNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
) (*workflowservice.ListNamespacesResponse, error) {

	scope := a.GetResource().GetMetricsClient().Scope(metrics.FrontendListNamespacesScope)

	attr := &authorization.Attributes{
		APIName: "ListNamespaces",
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListNamespaces(ctx, request)
}

// ListOpenWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendListOpenWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ListOpenWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
}

// ListWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendListWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ListWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListWorkflowExecutions(ctx, request)
}

// PollActivityTaskQueue API call
func (a *AccessControlledWorkflowHandler) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
) (*workflowservice.PollActivityTaskQueueResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendPollActivityTaskQueueScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "PollActivityTaskQueue",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.PollActivityTaskQueue(ctx, request)
}

// PollWorkflowTaskQueue API call
func (a *AccessControlledWorkflowHandler) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendPollWorkflowTaskQueueScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "PollWorkflowTaskQueue",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.PollWorkflowTaskQueue(ctx, request)
}

// QueryWorkflow API call
func (a *AccessControlledWorkflowHandler) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
) (*workflowservice.QueryWorkflowResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendQueryWorkflowScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "QueryWorkflow",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.QueryWorkflow(ctx, request)
}

// GetClusterInfo API call
func (a *AccessControlledWorkflowHandler) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
) (*workflowservice.GetClusterInfoResponse, error) {
	return a.frontendHandler.GetClusterInfo(ctx, request)
}

// RecordActivityTaskHeartbeat API call
func (a *AccessControlledWorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	// TODO(vancexu): add auth check for service API
	return a.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
}

// RecordActivityTaskHeartbeatById API call
func (a *AccessControlledWorkflowHandler) RecordActivityTaskHeartbeatById(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
) (*workflowservice.RecordActivityTaskHeartbeatByIdResponse, error) {
	return a.frontendHandler.RecordActivityTaskHeartbeatById(ctx, request)
}

// RegisterNamespace API call
func (a *AccessControlledWorkflowHandler) RegisterNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
) (*workflowservice.RegisterNamespaceResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendRegisterNamespaceScope, request.GetName())

	attr := &authorization.Attributes{
		APIName:   "RegisterNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.RegisterNamespace(ctx, request)
}

// RequestCancelWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendRequestCancelWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "RequestCancelWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
}

// ResetStickyTaskQueue API call
func (a *AccessControlledWorkflowHandler) ResetStickyTaskQueue(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskQueueRequest,
) (*workflowservice.ResetStickyTaskQueueResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendResetStickyTaskQueueScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ResetStickyTaskQueue",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ResetStickyTaskQueue(ctx, request)
}

// ResetWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendResetWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ResetWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ResetWorkflowExecution(ctx, request)
}

// RespondActivityTaskCanceled API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	return a.frontendHandler.RespondActivityTaskCanceled(ctx, request)
}

// RespondActivityTaskCanceledById API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCanceledById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIdRequest,
) (*workflowservice.RespondActivityTaskCanceledByIdResponse, error) {
	return a.frontendHandler.RespondActivityTaskCanceledById(ctx, request)
}

// RespondActivityTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return a.frontendHandler.RespondActivityTaskCompleted(ctx, request)
}

// RespondActivityTaskCompletedById API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCompletedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIdRequest,
) (*workflowservice.RespondActivityTaskCompletedByIdResponse, error) {
	return a.frontendHandler.RespondActivityTaskCompletedById(ctx, request)
}

// RespondActivityTaskFailed API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	return a.frontendHandler.RespondActivityTaskFailed(ctx, request)
}

// RespondActivityTaskFailedById API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskFailedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIdRequest,
) (*workflowservice.RespondActivityTaskFailedByIdResponse, error) {
	return a.frontendHandler.RespondActivityTaskFailedById(ctx, request)
}

// RespondWorkflowTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	return a.frontendHandler.RespondWorkflowTaskCompleted(ctx, request)
}

// RespondWorkflowTaskFailed API call
func (a *AccessControlledWorkflowHandler) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	return a.frontendHandler.RespondWorkflowTaskFailed(ctx, request)
}

// RespondQueryTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	return a.frontendHandler.RespondQueryTaskCompleted(ctx, request)
}

// ScanWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
) (*workflowservice.ScanWorkflowExecutionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendScanWorkflowExecutionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ScanWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ScanWorkflowExecutions(ctx, request)
}

// SignalWithStartWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendSignalWithStartWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "SignalWithStartWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
}

// SignalWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
) (*workflowservice.SignalWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendSignalWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "SignalWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.SignalWorkflowExecution(ctx, request)
}

// StartWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendStartWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "StartWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.StartWorkflowExecution(ctx, request)
}

// TerminateWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
) (*workflowservice.TerminateWorkflowExecutionResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendTerminateWorkflowExecutionScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "TerminateWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.TerminateWorkflowExecution(ctx, request)
}

// ListTaskQueuePartitions API call
func (a *AccessControlledWorkflowHandler) ListTaskQueuePartitions(
	ctx context.Context,
	request *workflowservice.ListTaskQueuePartitionsRequest,
) (*workflowservice.ListTaskQueuePartitionsResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendListTaskQueuePartitionsScope, request.GetNamespace())

	attr := &authorization.Attributes{
		APIName:   "ListTaskQueuePartitions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListTaskQueuePartitions(ctx, request)
}

// UpdateNamespace API call
func (a *AccessControlledWorkflowHandler) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {

	scope := a.getMetricsScopeWithNamespace(metrics.FrontendUpdateNamespaceScope, request.GetName())

	attr := &authorization.Attributes{
		APIName:   "UpdateNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr, scope)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.UpdateNamespace(ctx, request)
}

func (a *AccessControlledWorkflowHandler) isAuthorized(
	ctx context.Context,
	attr *authorization.Attributes,
	scope metrics.Scope,
) (bool, error) {
	sw := scope.StartTimer(metrics.ServiceAuthorizationLatency)
	defer sw.Stop()

	result, err := a.authorizer.Authorize(ctx, attr)
	if err != nil {
		scope.IncCounter(metrics.ServiceErrAuthorizeFailedCounter)
		return false, err
	}
	isAuth := result.Decision == authorization.DecisionAllow
	if !isAuth {
		scope.IncCounter(metrics.ServiceErrUnauthorizedCounter)
	}
	return isAuth, nil
}

// getMetricsScopeWithNamespace return metrics scope with namespace tag
func (a *AccessControlledWorkflowHandler) getMetricsScopeWithNamespace(
	scope int,
	namespace string,
) metrics.Scope {
	return getMetricsScopeWithNamespace(scope, namespace, a.GetResource().GetMetricsClient())
}

func getMetricsScopeWithNamespace(
	scope int,
	namespace string,
	metricsClient metrics.Client,
) metrics.Scope {
	var metricsScope metrics.Scope
	if namespace != "" {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespace))
	} else {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.NamespaceUnknownTag())
	}

	return metricsScope
}
