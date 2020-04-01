// Copyright (c) 2019 Uber Technologies, Inc.
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

	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/temporalio/temporal/common/authorization"
	"github.com/temporalio/temporal/common/resource"
)

// TODO(vancexu): add metrics

// AccessControlledWorkflowHandler frontend handler wrapper for authentication and authorization
type AccessControlledWorkflowHandler struct {
	resource.Resource

	frontendHandler workflowservice.WorkflowServiceServer
	authorizer      authorization.Authorizer
}

var _ workflowservice.WorkflowServiceServer = (*AccessControlledWorkflowHandler)(nil)

// NewAccessControlledHandlerImpl creates frontend handler with authentication support
func NewAccessControlledHandlerImpl(wfHandler *DCRedirectionHandlerImpl, authorizer authorization.Authorizer) *AccessControlledWorkflowHandler {
	if authorizer == nil {
		authorizer = authorization.NewNopAuthorizer()
	}

	return &AccessControlledWorkflowHandler{
		Resource:        wfHandler.Resource,
		frontendHandler: wfHandler,
		authorizer:      authorizer,
	}
}

// TODO(vancexu): refactor frontend handler

// https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func (a *AccessControlledWorkflowHandler) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	a.GetLogger().Debug("Frontend service health check endpoint (gRPC) reached.")
	hs := &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}
	return hs, nil
}
func (a *AccessControlledWorkflowHandler) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return serviceerror.NewUnimplemented("Watch is not implemented.")
}

// CountWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "CountWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "DeprecateNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "DescribeNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeNamespace(ctx, request)
}

// DescribeTaskList API call
func (a *AccessControlledWorkflowHandler) DescribeTaskList(
	ctx context.Context,
	request *workflowservice.DescribeTaskListRequest,
) (*workflowservice.DescribeTaskListResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "DescribeTaskList",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeTaskList(ctx, request)
}

// DescribeWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "DescribeWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "GetWorkflowExecutionHistory",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
}

// GetWorkflowExecutionRawHistory API call
func (a *AccessControlledWorkflowHandler) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionRawHistoryRequest,
) (*workflowservice.GetWorkflowExecutionRawHistoryResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "GetWorkflowExecutionRawHistory",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.GetWorkflowExecutionRawHistory(ctx, request)
}

// PollForWorkflowExecutionRawHistory API call
func (a *AccessControlledWorkflowHandler) PollForWorkflowExecutionRawHistory(
	ctx context.Context,
	request *workflowservice.PollForWorkflowExecutionRawHistoryRequest,
) (*workflowservice.PollForWorkflowExecutionRawHistoryResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "PollForWorkflowExecutionRawHistory",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.PollForWorkflowExecutionRawHistory(ctx, request)
}

// ListArchivedWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "ListArchivedWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "ListClosedWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName: "ListNamespaces",
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "ListOpenWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "ListWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListWorkflowExecutions(ctx, request)
}

// PollForActivityTask API call
func (a *AccessControlledWorkflowHandler) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
) (*workflowservice.PollForActivityTaskResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "PollForActivityTask",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.PollForActivityTask(ctx, request)
}

// PollForDecisionTask API call
func (a *AccessControlledWorkflowHandler) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
) (*workflowservice.PollForDecisionTaskResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "PollForDecisionTask",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.PollForDecisionTask(ctx, request)
}

// QueryWorkflow API call
func (a *AccessControlledWorkflowHandler) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
) (*workflowservice.QueryWorkflowResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "QueryWorkflow",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "RegisterNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "RequestCancelWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
}

// ResetStickyTaskList API call
func (a *AccessControlledWorkflowHandler) ResetStickyTaskList(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskListRequest,
) (*workflowservice.ResetStickyTaskListResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "ResetStickyTaskList",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ResetStickyTaskList(ctx, request)
}

// ResetWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
) (*workflowservice.ResetWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "ResetWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

// RespondDecisionTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
) (*workflowservice.RespondDecisionTaskCompletedResponse, error) {
	return a.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
}

// RespondDecisionTaskFailed API call
func (a *AccessControlledWorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
) (*workflowservice.RespondDecisionTaskFailedResponse, error) {
	return a.frontendHandler.RespondDecisionTaskFailed(ctx, request)
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

	attr := &authorization.Attributes{
		APIName:   "ScanWorkflowExecutions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "SignalWithStartWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "SignalWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "StartWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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

	attr := &authorization.Attributes{
		APIName:   "TerminateWorkflowExecution",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.TerminateWorkflowExecution(ctx, request)
}

// ListTaskListPartitions API call
func (a *AccessControlledWorkflowHandler) ListTaskListPartitions(
	ctx context.Context,
	request *workflowservice.ListTaskListPartitionsRequest,
) (*workflowservice.ListTaskListPartitionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "ListTaskListPartitions",
		Namespace: request.GetNamespace(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListTaskListPartitions(ctx, request)
}

// UpdateNamespace API call
func (a *AccessControlledWorkflowHandler) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {

	attr := &authorization.Attributes{
		APIName:   "UpdateNamespace",
		Namespace: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
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
) (bool, error) {
	result, err := a.authorizer.Authorize(ctx, attr)
	if err != nil {
		return false, err
	}
	return result.Decision == authorization.DecisionAllow, nil
}
