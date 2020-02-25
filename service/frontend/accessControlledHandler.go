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

	"github.com/uber/cadence/common/authorization"

	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/resource"
)

// TODO(vancexu): add metrics

var errUnauthorized = &shared.BadRequestError{Message: "Request unauthorized."}

// AccessControlledWorkflowHandler frontend handler wrapper for authentication and authorization
type AccessControlledWorkflowHandler struct {
	resource.Resource

	frontendHandler workflowserviceserver.Interface
	authorizer      authorization.Authorizer

	startFn func()
	stopFn  func()
}

var _ workflowserviceserver.Interface = (*AccessControlledWorkflowHandler)(nil)

// NewAccessControlledHandlerImpl creates frontend handler with authentication support
func NewAccessControlledHandlerImpl(wfHandler *DCRedirectionHandlerImpl, authorizer authorization.Authorizer) *AccessControlledWorkflowHandler {
	if authorizer == nil {
		authorizer = authorization.NewNopAuthorizer()
	}

	return &AccessControlledWorkflowHandler{
		Resource:        wfHandler.Resource,
		frontendHandler: wfHandler,
		authorizer:      authorizer,
		startFn:         func() { wfHandler.Start() },
		stopFn:          func() { wfHandler.Stop() },
	}
}

// TODO(vancexu): refactor frontend handler

// RegisterHandler register this handler, must be called before Start()
func (a *AccessControlledWorkflowHandler) RegisterHandler() {
	a.GetDispatcher().Register(workflowserviceserver.New(a))
	a.GetDispatcher().Register(metaserver.New(a))
}

// Health callback for for health check
func (a *AccessControlledWorkflowHandler) Health(ctx context.Context) (*health.HealthStatus, error) {
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("auth is good")}
	return hs, nil
}

// Start starts the handler
func (a *AccessControlledWorkflowHandler) Start() {
	a.startFn()
}

// Stop stops the handler
func (a *AccessControlledWorkflowHandler) Stop() {
	a.stopFn()
}

// CountWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) CountWorkflowExecutions(
	ctx context.Context,
	request *shared.CountWorkflowExecutionsRequest,
) (*shared.CountWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "CountWorkflowExecutions",
		DomainName: request.GetDomain(),
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

// DeprecateDomain API call
func (a *AccessControlledWorkflowHandler) DeprecateDomain(
	ctx context.Context,
	request *shared.DeprecateDomainRequest,
) error {

	attr := &authorization.Attributes{
		APIName:    "DeprecateDomain",
		DomainName: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return err
	}
	if !isAuthorized {
		return errUnauthorized
	}

	return a.frontendHandler.DeprecateDomain(ctx, request)
}

// DescribeDomain API call
func (a *AccessControlledWorkflowHandler) DescribeDomain(
	ctx context.Context,
	request *shared.DescribeDomainRequest,
) (*shared.DescribeDomainResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "DescribeDomain",
		DomainName: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.DescribeDomain(ctx, request)
}

// DescribeTaskList API call
func (a *AccessControlledWorkflowHandler) DescribeTaskList(
	ctx context.Context,
	request *shared.DescribeTaskListRequest,
) (*shared.DescribeTaskListResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "DescribeTaskList",
		DomainName: request.GetDomain(),
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
	request *shared.DescribeWorkflowExecutionRequest,
) (*shared.DescribeWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "DescribeWorkflowExecution",
		DomainName: request.GetDomain(),
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
) (*shared.GetSearchAttributesResponse, error) {
	return a.frontendHandler.GetSearchAttributes(ctx)
}

// GetWorkflowExecutionHistory API call
func (a *AccessControlledWorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "GetWorkflowExecutionHistory",
		DomainName: request.GetDomain(),
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
	request *shared.GetWorkflowExecutionRawHistoryRequest,
) (*shared.GetWorkflowExecutionRawHistoryResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "GetWorkflowExecutionRawHistory",
		DomainName: request.GetDomain(),
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
	request *shared.PollForWorkflowExecutionRawHistoryRequest,
) (*shared.PollForWorkflowExecutionRawHistoryResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "PollForWorkflowExecutionRawHistory",
		DomainName: request.GetDomain(),
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
	request *shared.ListArchivedWorkflowExecutionsRequest,
) (*shared.ListArchivedWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ListArchivedWorkflowExecutions",
		DomainName: request.GetDomain(),
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
	request *shared.ListClosedWorkflowExecutionsRequest,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ListClosedWorkflowExecutions",
		DomainName: request.GetDomain(),
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

// ListDomains API call
func (a *AccessControlledWorkflowHandler) ListDomains(
	ctx context.Context,
	request *shared.ListDomainsRequest,
) (*shared.ListDomainsResponse, error) {

	attr := &authorization.Attributes{
		APIName: "ListDomains",
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.ListDomains(ctx, request)
}

// ListOpenWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *shared.ListOpenWorkflowExecutionsRequest,
) (*shared.ListOpenWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ListOpenWorkflowExecutions",
		DomainName: request.GetDomain(),
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
	request *shared.ListWorkflowExecutionsRequest,
) (*shared.ListWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ListWorkflowExecutions",
		DomainName: request.GetDomain(),
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
	request *shared.PollForActivityTaskRequest,
) (*shared.PollForActivityTaskResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "PollForActivityTask",
		DomainName: request.GetDomain(),
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
	request *shared.PollForDecisionTaskRequest,
) (*shared.PollForDecisionTaskResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "PollForDecisionTask",
		DomainName: request.GetDomain(),
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
	request *shared.QueryWorkflowRequest,
) (*shared.QueryWorkflowResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "QueryWorkflow",
		DomainName: request.GetDomain(),
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
) (*shared.ClusterInfo, error) {
	return a.frontendHandler.GetClusterInfo(ctx)
}

// RecordActivityTaskHeartbeat API call
func (a *AccessControlledWorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatRequest,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	// TODO(vancexu): add auth check for service API
	return a.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
}

// RecordActivityTaskHeartbeatByID API call
func (a *AccessControlledWorkflowHandler) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	request *shared.RecordActivityTaskHeartbeatByIDRequest,
) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	return a.frontendHandler.RecordActivityTaskHeartbeatByID(ctx, request)
}

// RegisterDomain API call
func (a *AccessControlledWorkflowHandler) RegisterDomain(
	ctx context.Context,
	request *shared.RegisterDomainRequest,
) error {

	attr := &authorization.Attributes{
		APIName:    "RegisterDomain",
		DomainName: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return err
	}
	if !isAuthorized {
		return errUnauthorized
	}

	return a.frontendHandler.RegisterDomain(ctx, request)
}

// RequestCancelWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *shared.RequestCancelWorkflowExecutionRequest,
) error {

	attr := &authorization.Attributes{
		APIName:    "RequestCancelWorkflowExecution",
		DomainName: request.GetDomain(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return err
	}
	if !isAuthorized {
		return errUnauthorized
	}

	return a.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
}

// ResetStickyTaskList API call
func (a *AccessControlledWorkflowHandler) ResetStickyTaskList(
	ctx context.Context,
	request *shared.ResetStickyTaskListRequest,
) (*shared.ResetStickyTaskListResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ResetStickyTaskList",
		DomainName: request.GetDomain(),
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
	request *shared.ResetWorkflowExecutionRequest,
) (*shared.ResetWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ResetWorkflowExecution",
		DomainName: request.GetDomain(),
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
	request *shared.RespondActivityTaskCanceledRequest,
) error {
	return a.frontendHandler.RespondActivityTaskCanceled(ctx, request)
}

// RespondActivityTaskCanceledByID API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCanceledByIDRequest,
) error {
	return a.frontendHandler.RespondActivityTaskCanceledByID(ctx, request)
}

// RespondActivityTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedRequest,
) error {
	return a.frontendHandler.RespondActivityTaskCompleted(ctx, request)
}

// RespondActivityTaskCompletedByID API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskCompletedByIDRequest,
) error {
	return a.frontendHandler.RespondActivityTaskCompletedByID(ctx, request)
}

// RespondActivityTaskFailed API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedRequest,
) error {
	return a.frontendHandler.RespondActivityTaskFailed(ctx, request)
}

// RespondActivityTaskFailedByID API call
func (a *AccessControlledWorkflowHandler) RespondActivityTaskFailedByID(
	ctx context.Context,
	request *shared.RespondActivityTaskFailedByIDRequest,
) error {
	return a.frontendHandler.RespondActivityTaskFailedByID(ctx, request)
}

// RespondDecisionTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *shared.RespondDecisionTaskCompletedRequest,
) (*shared.RespondDecisionTaskCompletedResponse, error) {
	return a.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
}

// RespondDecisionTaskFailed API call
func (a *AccessControlledWorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	request *shared.RespondDecisionTaskFailedRequest,
) error {
	return a.frontendHandler.RespondDecisionTaskFailed(ctx, request)
}

// RespondQueryTaskCompleted API call
func (a *AccessControlledWorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *shared.RespondQueryTaskCompletedRequest,
) error {
	return a.frontendHandler.RespondQueryTaskCompleted(ctx, request)
}

// ScanWorkflowExecutions API call
func (a *AccessControlledWorkflowHandler) ScanWorkflowExecutions(
	ctx context.Context,
	request *shared.ListWorkflowExecutionsRequest,
) (*shared.ListWorkflowExecutionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ScanWorkflowExecutions",
		DomainName: request.GetDomain(),
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
	request *shared.SignalWithStartWorkflowExecutionRequest,
) (*shared.StartWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "SignalWithStartWorkflowExecution",
		DomainName: request.GetDomain(),
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
	request *shared.SignalWorkflowExecutionRequest,
) error {

	attr := &authorization.Attributes{
		APIName:    "SignalWorkflowExecution",
		DomainName: request.GetDomain(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return err
	}
	if !isAuthorized {
		return errUnauthorized
	}

	return a.frontendHandler.SignalWorkflowExecution(ctx, request)
}

// StartWorkflowExecution API call
func (a *AccessControlledWorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	request *shared.StartWorkflowExecutionRequest,
) (*shared.StartWorkflowExecutionResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "StartWorkflowExecution",
		DomainName: request.GetDomain(),
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
	request *shared.TerminateWorkflowExecutionRequest,
) error {

	attr := &authorization.Attributes{
		APIName:    "TerminateWorkflowExecution",
		DomainName: request.GetDomain(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return err
	}
	if !isAuthorized {
		return errUnauthorized
	}

	return a.frontendHandler.TerminateWorkflowExecution(ctx, request)
}

// ListTaskListPartitions API call
func (a *AccessControlledWorkflowHandler) ListTaskListPartitions(
	ctx context.Context,
	request *shared.ListTaskListPartitionsRequest,
) (*shared.ListTaskListPartitionsResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "ListTaskListPartitions",
		DomainName: request.GetDomain(),
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

// UpdateDomain API call
func (a *AccessControlledWorkflowHandler) UpdateDomain(
	ctx context.Context,
	request *shared.UpdateDomainRequest,
) (*shared.UpdateDomainResponse, error) {

	attr := &authorization.Attributes{
		APIName:    "UpdateDomain",
		DomainName: request.GetName(),
	}
	isAuthorized, err := a.isAuthorized(ctx, attr)
	if err != nil {
		return nil, err
	}
	if !isAuthorized {
		return nil, errUnauthorized
	}

	return a.frontendHandler.UpdateDomain(ctx, request)
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
