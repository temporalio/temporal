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

	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

var _ Handler = (*DCRedirectionHandlerImpl)(nil)

type (
	// DCRedirectionHandlerImpl is simple wrapper over frontend service, doing redirection based on policy
	DCRedirectionHandlerImpl struct {
		currentClusterName string
		config             *Config
		redirectionPolicy  DCRedirectionPolicy
		tokenSerializer    common.TaskTokenSerializer
		frontendHandler    Handler
		logger             log.Logger
		clientBean         client.Bean
		metricsClient      metrics.Client
		timeSource         clock.TimeSource
	}
)

// NewDCRedirectionHandler creates a thrift handler for the temporal service, frontend
func NewDCRedirectionHandler(
	wfHandler Handler,
	policy config.DCRedirectionPolicy,
	logger log.Logger,
	clientBean client.Bean,
	metricsClient metrics.Client,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
) *DCRedirectionHandlerImpl {
	dcRedirectionPolicy := RedirectionPolicyGenerator(
		clusterMetadata,
		wfHandler.GetConfig(),
		namespaceRegistry,
		policy,
	)

	return &DCRedirectionHandlerImpl{
		currentClusterName: clusterMetadata.GetCurrentClusterName(),
		config:             wfHandler.GetConfig(),
		redirectionPolicy:  dcRedirectionPolicy,
		tokenSerializer:    common.NewProtoTaskTokenSerializer(),
		frontendHandler:    wfHandler,
		logger:             logger,
		clientBean:         clientBean,
		metricsClient:      metricsClient,
		timeSource:         timeSource,
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

// GetConfig return config
func (handler *DCRedirectionHandlerImpl) GetConfig() *Config {
	return handler.frontendHandler.GetConfig()
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeTaskQueue(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkflowExecutionHistory(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.GetWorkflowExecutionHistory(ctx, request)
		}
		return err
	})

	return resp, err
}

// GetWorkflowExecutionHistoryReverse API call
func (handler *DCRedirectionHandlerImpl) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
) (resp *workflowservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {

	var apiName = "GetWorkflowExecutionHistoryReverse"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetWorkflowExecutionHistoryReverseScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkflowExecutionHistoryReverse(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.GetWorkflowExecutionHistoryReverse(ctx, request)
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListArchivedWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListClosedWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListOpenWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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
	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ScanWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.CountWorkflowExecutions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

// PollActivityTaskQueue API call
func (handler *DCRedirectionHandlerImpl) PollActivityTaskQueue(
	ctx context.Context,
	request *workflowservice.PollActivityTaskQueueRequest,
) (resp *workflowservice.PollActivityTaskQueueResponse, retError error) {

	var apiName = "PollActivityTaskQueue"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollActivityTaskQueueScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollActivityTaskQueue(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.PollActivityTaskQueue(ctx, request)
		}
		return err
	})

	return resp, err
}

// PollWorkflowTaskQueue API call
func (handler *DCRedirectionHandlerImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
) (resp *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

	var apiName = "PollWorkflowTaskQueue"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPollWorkflowTaskQueueScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PollWorkflowTaskQueue(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.PollWorkflowTaskQueue(ctx, request)
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.QueryWorkflow(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeat(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RecordActivityTaskHeartbeatById(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RequestCancelWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetStickyTaskQueue(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ResetWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCanceled(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCanceledById(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCompleted(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskCompletedById(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskFailed(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondActivityTaskFailedById(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.RespondActivityTaskFailedById(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondWorkflowTaskCompleted API call
func (handler *DCRedirectionHandlerImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (resp *workflowservice.RespondWorkflowTaskCompletedResponse, retError error) {

	var apiName = "RespondWorkflowTaskCompleted"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondWorkflowTaskCompletedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondWorkflowTaskCompleted(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.RespondWorkflowTaskCompleted(ctx, request)
		}
		return err
	})

	return resp, err
}

// RespondWorkflowTaskFailed API call
func (handler *DCRedirectionHandlerImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
) (resp *workflowservice.RespondWorkflowTaskFailedResponse, retError error) {

	var apiName = "RespondWorkflowTaskFailed"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionRespondWorkflowTaskFailedScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return resp, err
	}

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondWorkflowTaskFailed(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.RespondWorkflowTaskFailed(ctx, request)
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

	err = handler.redirectionPolicy.WithNamespaceIDRedirect(ctx, namespace.ID(token.GetNamespaceId()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.RespondQueryTaskCompleted(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWithStartWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.SignalWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.StartWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.TerminateWorkflowExecution(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListTaskQueuePartitions(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
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

// GetSystemInfo API call
func (handler *DCRedirectionHandlerImpl) GetSystemInfo(
	ctx context.Context,
	request *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return handler.frontendHandler.GetSystemInfo(ctx, request)
}

// CreateSchedule API call
func (handler *DCRedirectionHandlerImpl) CreateSchedule(
	ctx context.Context,
	request *workflowservice.CreateScheduleRequest,
) (resp *workflowservice.CreateScheduleResponse, retError error) {
	var apiName = "CreateSchedule"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionCreateScheduleScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.CreateSchedule(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.CreateSchedule(ctx, request)
		}
		return err
	})

	return resp, err
}

// DescribeSchedule API call
func (handler *DCRedirectionHandlerImpl) DescribeSchedule(
	ctx context.Context,
	request *workflowservice.DescribeScheduleRequest,
) (resp *workflowservice.DescribeScheduleResponse, retError error) {
	var apiName = "DescribeSchedule"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDescribeScheduleScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DescribeSchedule(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.DescribeSchedule(ctx, request)
		}
		return err
	})

	return resp, err
}

// UpdateSchedule API call
func (handler *DCRedirectionHandlerImpl) UpdateSchedule(
	ctx context.Context,
	request *workflowservice.UpdateScheduleRequest,
) (resp *workflowservice.UpdateScheduleResponse, retError error) {
	var apiName = "UpdateSchedule"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionUpdateScheduleScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.UpdateSchedule(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.UpdateSchedule(ctx, request)
		}
		return err
	})

	return resp, err
}

// PatchSchedule API call
func (handler *DCRedirectionHandlerImpl) PatchSchedule(
	ctx context.Context,
	request *workflowservice.PatchScheduleRequest,
) (resp *workflowservice.PatchScheduleResponse, retError error) {
	var apiName = "PatchSchedule"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionPatchScheduleScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.PatchSchedule(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.PatchSchedule(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListScheduleMatchingTimes API call
func (handler *DCRedirectionHandlerImpl) ListScheduleMatchingTimes(
	ctx context.Context,
	request *workflowservice.ListScheduleMatchingTimesRequest,
) (resp *workflowservice.ListScheduleMatchingTimesResponse, retError error) {
	var apiName = "ListScheduleMatchingTimes"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListScheduleMatchingTimesScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListScheduleMatchingTimes(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.ListScheduleMatchingTimes(ctx, request)
		}
		return err
	})

	return resp, err
}

// DeleteSchedule API call
func (handler *DCRedirectionHandlerImpl) DeleteSchedule(
	ctx context.Context,
	request *workflowservice.DeleteScheduleRequest,
) (resp *workflowservice.DeleteScheduleResponse, retError error) {
	var apiName = "DeleteSchedule"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionDeleteScheduleScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.DeleteSchedule(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.DeleteSchedule(ctx, request)
		}
		return err
	})

	return resp, err
}

// ListSchedules API call
func (handler *DCRedirectionHandlerImpl) ListSchedules(
	ctx context.Context,
	request *workflowservice.ListSchedulesRequest,
) (resp *workflowservice.ListSchedulesResponse, retError error) {
	var apiName = "ListSchedules"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionListSchedulesScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.ListSchedules(ctx, request)
		default:
			var remoteClient workflowservice.WorkflowServiceClient
			remoteClient, err = handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.ListSchedules(ctx, request)
		}
		return err
	})

	return resp, err
}

// UpdateWorkerBuildIdOrdering API call
func (handler *DCRedirectionHandlerImpl) UpdateWorkerBuildIdOrdering(
	ctx context.Context,
	request *workflowservice.UpdateWorkerBuildIdOrderingRequest,
) (resp *workflowservice.UpdateWorkerBuildIdOrderingResponse, retError error) {
	const apiName = "UpdateWorkerBuildIdOrdering"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionUpdateWorkerBuildIdOrderingScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC

		if targetDC == handler.currentClusterName {
			resp, err = handler.frontendHandler.UpdateWorkerBuildIdOrdering(ctx, request)
			return err
		} else {
			remoteClient, err := handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.UpdateWorkerBuildIdOrdering(ctx, request)
			return err
		}
	})

	return resp, err
}

// GetWorkerBuildIdOrdering API call
func (handler *DCRedirectionHandlerImpl) GetWorkerBuildIdOrdering(
	ctx context.Context,
	request *workflowservice.GetWorkerBuildIdOrderingRequest,
) (resp *workflowservice.GetWorkerBuildIdOrderingResponse, retError error) {
	var apiName = "GetWorkerBuildIdOrdering"
	var err error
	var cluster string

	scope, startTime := handler.beforeCall(metrics.DCRedirectionGetWorkerBuildIdOrderingScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), apiName, func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.GetWorkerBuildIdOrdering(ctx, request)
			return err
		default:
			remoteClient, err := handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.GetWorkerBuildIdOrdering(ctx, request)
			return err
		}
	})

	return resp, err
}

// UpdateWorkflow API call
func (handler *DCRedirectionHandlerImpl) UpdateWorkflow(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowRequest,
) (resp *workflowservice.UpdateWorkflowResponse, retError error) {
	var (
		err     error
		cluster string
	)

	scope, startTime := handler.beforeCall(metrics.DCRedirectionUpdateWorkflowScope)
	defer func() {
		handler.afterCall(scope, startTime, cluster, &retError)
	}()

	err = handler.redirectionPolicy.WithNamespaceRedirect(ctx, namespace.Name(request.GetNamespace()), "UpdateWorkflow", func(targetDC string) error {
		cluster = targetDC
		switch {
		case targetDC == handler.currentClusterName:
			resp, err = handler.frontendHandler.UpdateWorkflow(ctx, request)
			return err
		default:
			remoteClient, err := handler.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp, err = remoteClient.UpdateWorkflow(ctx, request)
			return err
		}
	})

	return resp, err
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

	log.CapturePanic(handler.logger, retError)

	scope = scope.Tagged(metrics.TargetClusterTag(cluster))
	scope.IncCounter(metrics.ClientRedirectionRequests)
	scope.RecordTimer(metrics.ClientRedirectionLatency, handler.timeSource.Now().Sub(startTime))
	if *retError != nil {
		scope.IncCounter(metrics.ClientRedirectionFailures)
	}
}
