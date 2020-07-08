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
	"time"

	"go.temporal.io/temporal-proto/workflowservice/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/config"
)

var _ Handler = (*DCRedirectionHandlerImpl)(nil)

type (
	// DCRedirectionHandlerImpl is simple wrapper over frontend service, doing redirection based on policy
	DCRedirectionHandlerImpl struct {
		resource.Resource

		currentClusterName string
		config             *Config
		redirectionPolicy  DCRedirectionPolicy
		tokenSerializer    common.TaskTokenSerializer
		frontendHandler    Handler
	}
)

// NewDCRedirectionHandler creates a thrift handler for the temporal service, frontend
func NewDCRedirectionHandler(
	wfHandler Handler,
	policy config.DCRedirectionPolicy,
) *DCRedirectionHandlerImpl {
	resource := wfHandler.GetResource()
	dcRedirectionPolicy := RedirectionPolicyGenerator(
		resource.GetClusterMetadata(),
		wfHandler.GetConfig(),
		resource.GetNamespaceCache(),
		policy,
	)

	return &DCRedirectionHandlerImpl{
		Resource:           resource,
		currentClusterName: resource.GetClusterMetadata().GetCurrentClusterName(),
		config:             wfHandler.GetConfig(),
		redirectionPolicy:  dcRedirectionPolicy,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		frontendHandler:    wfHandler,
	}
}

// Start starts the handler
func (handler *DCRedirectionHandlerImpl) Start() {
	handler.frontendHandler.Start()
}

// Stop stops the handler
func (handler *DCRedirectionHandlerImpl) Stop() {
	handler.frontendHandler.Stop()
}

// GetResource return resource
func (handler *DCRedirectionHandlerImpl) GetResource() resource.Resource {
	return handler.Resource
}

// GetConfig return config
func (handler *DCRedirectionHandlerImpl) GetConfig() *Config {
	return handler.frontendHandler.GetConfig()
}

// UpdateHealthStatus sets the health status for this rpc handler.
// This health status will be used within the rpc health check handler
func (handler *DCRedirectionHandlerImpl) UpdateHealthStatus(status HealthStatus) {
	handler.frontendHandler.UpdateHealthStatus(status)
}

// Check is for health check
func (handler *DCRedirectionHandlerImpl) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return handler.frontendHandler.Check(ctx, request)
}

func (handler *DCRedirectionHandlerImpl) Watch(request *healthpb.HealthCheckRequest, server healthpb.Health_WatchServer) error {
	return handler.frontendHandler.Watch(request, server)
}

// Namespace APIs, namespace APIs does not require redirection

// DeprecateNamespace API call
func (handler *DCRedirectionHandlerImpl) DeprecateNamespace(
	ctx context.Context,
	request *workflowservice.DeprecateNamespaceRequest,
) (resp *workflowservice.DeprecateNamespaceResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDeprecateNamespaceScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.DeprecateNamespace(ctx, request)
}

// DescribeNamespace API call
func (handler *DCRedirectionHandlerImpl) DescribeNamespace(
	ctx context.Context,
	request *workflowservice.DescribeNamespaceRequest,
) (resp *workflowservice.DescribeNamespaceResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeNamespaceScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.DescribeNamespace(ctx, request)
}

// ListNamespaces API call
func (handler *DCRedirectionHandlerImpl) ListNamespaces(
	ctx context.Context,
	request *workflowservice.ListNamespacesRequest,
) (resp *workflowservice.ListNamespacesResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListNamespacesScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.ListNamespaces(ctx, request)
}

// RegisterNamespace API call
func (handler *DCRedirectionHandlerImpl) RegisterNamespace(
	ctx context.Context,
	request *workflowservice.RegisterNamespaceRequest,
) (resp *workflowservice.RegisterNamespaceResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRegisterNamespaceScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.RegisterNamespace(ctx, request)
}

// UpdateNamespace API call
func (handler *DCRedirectionHandlerImpl) UpdateNamespace(
	ctx context.Context,
	request *workflowservice.UpdateNamespaceRequest,
) (resp *workflowservice.UpdateNamespaceResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionUpdateNamespaceScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.UpdateNamespace(ctx, request)
}

// Other APIs

// DescribeTaskQueue API call
func (handler *DCRedirectionHandlerImpl) DescribeTaskQueue(
	ctx context.Context,
	request *workflowservice.DescribeTaskQueueRequest,
) (resp *workflowservice.DescribeTaskQueueResponse, retError error) {

	var apiName = "DescribeTaskQueue"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeTaskQueueScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeTaskQueue(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.DescribeTaskQueue(ctx, request)
		}
		return err
	})

	return resp, err
}

// DescribeWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *workflowservice.DescribeWorkflowExecutionRequest,
) (resp *workflowservice.DescribeWorkflowExecutionResponse, retError error) {

	var apiName = "DescribeWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.DescribeWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetWorkflowExecutionHistory API call
func (handler *DCRedirectionHandlerImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
) (resp *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {

	var apiName = "GetWorkflowExecutionHistory"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetWorkflowExecutionHistoryScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.GetWorkflowExecutionHistory(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListArchivedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListArchivedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListArchivedWorkflowExecutionsRequest,
) (resp *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {

	var apiName = "ListArchivedWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListArchivedWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListArchivedWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListArchivedWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListClosedWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListClosedWorkflowExecutionsRequest,
) (resp *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {

	var apiName = "ListClosedWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListClosedWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListClosedWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListOpenWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListOpenWorkflowExecutionsRequest,
) (resp *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {

	var apiName = "ListOpenWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListOpenWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListOpenWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (resp *workflowservice.ListWorkflowExecutionsResponse, retError error) {

	var apiName = "ListWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// ScanWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) ScanWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.ScanWorkflowExecutionsRequest,
) (resp *workflowservice.ScanWorkflowExecutionsResponse, retError error) {

	var apiName = "ScanWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionScanWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()
	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ScanWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ScanWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// CountWorkflowExecutions API call
func (handler *DCRedirectionHandlerImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
) (resp *workflowservice.CountWorkflowExecutionsResponse, retError error) {

	var apiName = "CountWorkflowExecutions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionCountWorkflowExecutionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.CountWorkflowExecutions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.CountWorkflowExecutions(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetSearchAttributes API call
func (handler *DCRedirectionHandlerImpl) GetSearchAttributes(
	ctx context.Context,
	request *workflowservice.GetSearchAttributesRequest,
) (resp *workflowservice.GetSearchAttributesResponse, retError error) {

	var cluster = handler.currentClusterName

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetSearchAttributesScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	return handler.frontendHandler.GetSearchAttributes(ctx, request)
}

// PollForActivityTask API call
func (handler *DCRedirectionHandlerImpl) PollForActivityTask(
	ctx context.Context,
	request *workflowservice.PollForActivityTaskRequest,
) (resp *workflowservice.PollForActivityTaskResponse, retError error) {

	var apiName = "PollForActivityTask"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollForActivityTaskScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForActivityTask(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.PollForActivityTask(ctx, request)
		}
		return err
	})

	return resp, err
}

// PollForDecisionTask API call
func (handler *DCRedirectionHandlerImpl) PollForDecisionTask(
	ctx context.Context,
	request *workflowservice.PollForDecisionTaskRequest,
) (resp *workflowservice.PollForDecisionTaskResponse, retError error) {

	var apiName = "PollForDecisionTask"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollForDecisionTaskScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollForDecisionTask(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.PollForDecisionTask(ctx, request)
		}
		return err
	})

	return resp, err
}

// QueryWorkflow API call
func (handler *DCRedirectionHandlerImpl) QueryWorkflow(
	ctx context.Context,
	request *workflowservice.QueryWorkflowRequest,
) (resp *workflowservice.QueryWorkflowResponse, retError error) {

	var apiName = "QueryWorkflow"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionQueryWorkflowScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.QueryWorkflow(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.QueryWorkflow(ctx, request)
		}
		return err
	})

	return resp, err
}

// RecordActivityTaskHeartbeat API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatRequest,
) (resp *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {

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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RecordActivityTaskHeartbeat(ctx, request)
		}
		return err
	})

	return resp, err
}

// RecordActivityTaskHeartbeatById API call
func (handler *DCRedirectionHandlerImpl) RecordActivityTaskHeartbeatById(
	ctx context.Context,
	request *workflowservice.RecordActivityTaskHeartbeatByIdRequest,
) (resp *workflowservice.RecordActivityTaskHeartbeatByIdResponse, retError error) {

	var apiName = "RecordActivityTaskHeartbeatById"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRecordActivityTaskHeartbeatByIdScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeatById(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RecordActivityTaskHeartbeatById(ctx, request)
		}
		return err
	})

	return resp, err
}

// RequestCancelWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *workflowservice.RequestCancelWorkflowExecutionRequest,
) (resp *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {

	var apiName = "RequestCancelWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRequestCancelWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RequestCancelWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// ResetStickyTaskQueue API call
func (handler *DCRedirectionHandlerImpl) ResetStickyTaskQueue(
	ctx context.Context,
	request *workflowservice.ResetStickyTaskQueueRequest,
) (resp *workflowservice.ResetStickyTaskQueueResponse, retError error) {

	var apiName = "ResetStickyTaskQueue"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionResetStickyTaskQueueScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetStickyTaskQueue(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ResetStickyTaskQueue(ctx, request)
		}
		return err
	})

	return resp, err
}

// ResetWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
) (resp *workflowservice.ResetWorkflowExecutionResponse, retError error) {

	var apiName = "ResetWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionResetWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ResetWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskCanceled API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledRequest,
) (resp *workflowservice.RespondActivityTaskCanceledResponse, retError error) {

	var apiName = "RespondActivityTaskCanceled"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCanceledScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskCanceled(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskCanceledById API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCanceledById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCanceledByIdRequest,
) (resp *workflowservice.RespondActivityTaskCanceledByIdResponse, retError error) {

	var apiName = "RespondActivityTaskCanceledById"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCanceledByIdScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCanceledById(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskCanceledById(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (resp *workflowservice.RespondActivityTaskCompletedResponse, retError error) {

	var apiName = "RespondActivityTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskCompleted(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskCompletedById API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskCompletedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedByIdRequest,
) (resp *workflowservice.RespondActivityTaskCompletedByIdResponse, retError error) {

	var apiName = "RespondActivityTaskCompletedById"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskCompletedByIdScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCompletedById(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskCompletedById(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
) (resp *workflowservice.RespondActivityTaskFailedResponse, retError error) {

	var apiName = "RespondActivityTaskFailed"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskFailedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskFailed(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondActivityTaskFailedById API call
func (handler *DCRedirectionHandlerImpl) RespondActivityTaskFailedById(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedByIdRequest,
) (resp *workflowservice.RespondActivityTaskFailedByIdResponse, retError error) {

	var apiName = "RespondActivityTaskFailedById"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondActivityTaskFailedByIdScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskFailedById(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondActivityTaskFailedById(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondDecisionTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskCompletedRequest,
) (resp *workflowservice.RespondDecisionTaskCompletedResponse, retError error) {

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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondDecisionTaskCompleted(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondDecisionTaskCompleted(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondDecisionTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondDecisionTaskFailedRequest,
) (resp *workflowservice.RespondDecisionTaskFailedResponse, retError error) {

	var apiName = "RespondDecisionTaskFailed"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondDecisionTaskFailedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondDecisionTaskFailed(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondDecisionTaskFailed(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondQueryTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
) (resp *workflowservice.RespondQueryTaskCompletedResponse, retError error) {

	var apiName = "RespondQueryTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondQueryTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, token.GetNamespaceId(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.RespondQueryTaskCompleted(ctx, request)
		}
		return err
	})

	return resp, err
}

// SignalWithStartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (resp *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {

	var apiName = "SignalWithStartWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionSignalWithStartWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.SignalWithStartWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// SignalWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *workflowservice.SignalWorkflowExecutionRequest,
) (resp *workflowservice.SignalWorkflowExecutionResponse, retError error) {

	var apiName = "SignalWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionSignalWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.SignalWorkflowExecution(ctx, request)
		}
		return err
	})
	return resp, err
}

// StartWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
) (resp *workflowservice.StartWorkflowExecutionResponse, retError error) {

	var apiName = "StartWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionStartWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.StartWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.StartWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// TerminateWorkflowExecution API call
func (handler *DCRedirectionHandlerImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.TerminateWorkflowExecutionRequest,
) (resp *workflowservice.TerminateWorkflowExecutionResponse, retError error) {

	var apiName = "TerminateWorkflowExecution"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionTerminateWorkflowExecutionScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.TerminateWorkflowExecution(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListTaskQueuePartitions API call
func (handler *DCRedirectionHandlerImpl) ListTaskQueuePartitions(
	ctx context.Context,
	request *workflowservice.ListTaskQueuePartitionsRequest,
) (resp *workflowservice.ListTaskQueuePartitionsResponse, retError error) {

	var apiName = "ListTaskQueuePartitions"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListTaskQueuePartitionsScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, request.GetNamespace(), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListTaskQueuePartitions(ctx, request)
		default:
			remoteClient := handler.GetRemoteFrontendClient(targetDC)
			resp, err = remoteClient.ListTaskQueuePartitions(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetClusterInfo API call
func (handler *DCRedirectionHandlerImpl) GetClusterInfo(
	ctx context.Context,
	request *workflowservice.GetClusterInfoRequest,
) (*workflowservice.GetClusterInfoResponse, error) {
	return handler.frontendHandler.GetClusterInfo(ctx, request)
}

func (handler *DCRedirectionHandlerImpl) beforeCall(
	scope int,
) (metrics.Scope, time.Time) {

	return handler.GetMetricsClient().Scope(scope), handler.GetTimeSource().Now()
}

func (handler *DCRedirectionHandlerImpl) afterCall(
	scope metrics.Scope,
	startTime time.Time,
	cluster string,
	retError *error,
) {

	log.CapturePanic(handler.GetLogger(), retError)

	scope = scope.Tagged(metrics.TargetClusterTag(cluster))
	scope.IncCounter(metrics.ClientRedirectionRequests)
	scope.RecordTimer(metrics.ClientRedirectionLatency, handler.GetTimeSource().Now().Sub(startTime))
	if *retError != nil {
		scope.IncCounter(metrics.ClientRedirectionFailures)
	}
}
