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
	"strconv"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor"
)

const (
	dcRedirectionContextHeaderName = "xdc-redirection"

	dcRedirectionMetricsPrefix = "DCRedirection"
)

var (
	localAPIResponses = map[string]responseConstructorFn{
		// Namespace APIs, namespace APIs does not require redirection
		"DeprecateNamespace": func() any { return &workflowservice.DeprecateNamespaceResponse{} },
		"DescribeNamespace":  func() any { return &workflowservice.DescribeNamespaceResponse{} },
		"ListNamespaces":     func() any { return &workflowservice.ListNamespacesResponse{} },
		"RegisterNamespace":  func() any { return &workflowservice.RegisterNamespaceResponse{} },
		"UpdateNamespace":    func() any { return &workflowservice.UpdateNamespaceResponse{} },

		// Cluster info APIs, Cluster info APIs does not require redirection
		"GetSearchAttributes": func() any { return &workflowservice.GetSearchAttributesResponse{} },
		"GetClusterInfo":      func() any { return &workflowservice.GetClusterInfoResponse{} },
		"GetSystemInfo":       func() any { return &workflowservice.GetSystemInfoResponse{} },
	}

	globalAPIResponses = map[string]responseConstructorFn{
		"DescribeTaskQueue":                  func() any { return &workflowservice.DescribeTaskQueueResponse{} },
		"DescribeWorkflowExecution":          func() any { return &workflowservice.DescribeWorkflowExecutionResponse{} },
		"GetWorkflowExecutionHistory":        func() any { return &workflowservice.GetWorkflowExecutionHistoryResponse{} },
		"GetWorkflowExecutionHistoryReverse": func() any { return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{} },
		"ListArchivedWorkflowExecutions":     func() any { return &workflowservice.ListArchivedWorkflowExecutionsResponse{} },
		"ListClosedWorkflowExecutions":       func() any { return &workflowservice.ListClosedWorkflowExecutionsResponse{} },
		"ListOpenWorkflowExecutions":         func() any { return &workflowservice.ListOpenWorkflowExecutionsResponse{} },
		"ListWorkflowExecutions":             func() any { return &workflowservice.ListWorkflowExecutionsResponse{} },
		"ScanWorkflowExecutions":             func() any { return &workflowservice.ScanWorkflowExecutionsResponse{} },
		"CountWorkflowExecutions":            func() any { return &workflowservice.CountWorkflowExecutionsResponse{} },
		"PollActivityTaskQueue":              func() any { return &workflowservice.PollActivityTaskQueueResponse{} },
		"PollWorkflowTaskQueue":              func() any { return &workflowservice.PollWorkflowTaskQueueResponse{} },
		"QueryWorkflow":                      func() any { return &workflowservice.QueryWorkflowResponse{} },
		"RecordActivityTaskHeartbeat":        func() any { return &workflowservice.RecordActivityTaskHeartbeatResponse{} },
		"RecordActivityTaskHeartbeatById":    func() any { return &workflowservice.RecordActivityTaskHeartbeatByIdResponse{} },
		"RequestCancelWorkflowExecution":     func() any { return &workflowservice.RequestCancelWorkflowExecutionResponse{} },
		"ResetStickyTaskQueue":               func() any { return &workflowservice.ResetStickyTaskQueueResponse{} },
		"ResetWorkflowExecution":             func() any { return &workflowservice.ResetWorkflowExecutionResponse{} },
		"RespondActivityTaskCanceled":        func() any { return &workflowservice.RespondActivityTaskCanceledResponse{} },
		"RespondActivityTaskCanceledById":    func() any { return &workflowservice.RespondActivityTaskCanceledByIdResponse{} },
		"RespondActivityTaskCompleted":       func() any { return &workflowservice.RespondActivityTaskCompletedResponse{} },
		"RespondActivityTaskCompletedById":   func() any { return &workflowservice.RespondActivityTaskCompletedByIdResponse{} },
		"RespondActivityTaskFailed":          func() any { return &workflowservice.RespondActivityTaskFailedResponse{} },
		"RespondActivityTaskFailedById":      func() any { return &workflowservice.RespondActivityTaskFailedByIdResponse{} },
		"RespondWorkflowTaskCompleted":       func() any { return &workflowservice.RespondWorkflowTaskCompletedResponse{} },
		"RespondWorkflowTaskFailed":          func() any { return &workflowservice.RespondWorkflowTaskFailedResponse{} },
		"RespondQueryTaskCompleted":          func() any { return &workflowservice.RespondQueryTaskCompletedResponse{} },
		"SignalWithStartWorkflowExecution":   func() any { return &workflowservice.SignalWithStartWorkflowExecutionResponse{} },
		"SignalWorkflowExecution":            func() any { return &workflowservice.SignalWorkflowExecutionResponse{} },
		"StartWorkflowExecution":             func() any { return &workflowservice.StartWorkflowExecutionResponse{} },
		"UpdateWorkflowExecution":            func() any { return &workflowservice.UpdateWorkflowExecutionResponse{} },
		"PollWorkflowExecutionUpdate":        func() any { return &workflowservice.PollWorkflowExecutionUpdateResponse{} },
		"TerminateWorkflowExecution":         func() any { return &workflowservice.TerminateWorkflowExecutionResponse{} },
		"DeleteWorkflowExecution":            func() any { return &workflowservice.DeleteWorkflowExecutionResponse{} },
		"ListTaskQueuePartitions":            func() any { return &workflowservice.ListTaskQueuePartitionsResponse{} },

		"CreateSchedule":                   func() any { return &workflowservice.CreateScheduleResponse{} },
		"DescribeSchedule":                 func() any { return &workflowservice.DescribeScheduleResponse{} },
		"UpdateSchedule":                   func() any { return &workflowservice.UpdateScheduleResponse{} },
		"PatchSchedule":                    func() any { return &workflowservice.PatchScheduleResponse{} },
		"DeleteSchedule":                   func() any { return &workflowservice.DeleteScheduleResponse{} },
		"ListSchedules":                    func() any { return &workflowservice.ListSchedulesResponse{} },
		"ListScheduleMatchingTimes":        func() any { return &workflowservice.ListScheduleMatchingTimesResponse{} },
		"UpdateWorkerBuildIdCompatibility": func() any { return &workflowservice.UpdateWorkerBuildIdCompatibilityResponse{} },
		"GetWorkerBuildIdCompatibility":    func() any { return &workflowservice.GetWorkerBuildIdCompatibilityResponse{} },
		"GetWorkerTaskReachability":        func() any { return &workflowservice.GetWorkerTaskReachabilityResponse{} },

		"StartBatchOperation":    func() any { return &workflowservice.StartBatchOperationResponse{} },
		"StopBatchOperation":     func() any { return &workflowservice.StopBatchOperationResponse{} },
		"DescribeBatchOperation": func() any { return &workflowservice.DescribeBatchOperationResponse{} },
		"ListBatchOperations":    func() any { return &workflowservice.ListBatchOperationsResponse{} },
	}
)

type (
	responseConstructorFn func() any

	// RedirectionInterceptor is simple wrapper over frontend service, doing redirection based on policy
	RedirectionInterceptor struct {
		currentClusterName string
		config             *Config
		namespaceCache     namespace.Registry
		redirectionPolicy  DCRedirectionPolicy
		logger             log.Logger
		clientBean         client.Bean
		metricsHandler     metrics.Handler
		timeSource         clock.TimeSource
	}
)

// NewRedirectionInterceptor creates DC redirection interceptor
func NewRedirectionInterceptor(
	configuration *Config,
	namespaceCache namespace.Registry,
	policy config.DCRedirectionPolicy,
	logger log.Logger,
	clientBean client.Bean,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	clusterMetadata cluster.Metadata,
) *RedirectionInterceptor {
	dcRedirectionPolicy := RedirectionPolicyGenerator(
		clusterMetadata,
		configuration,
		namespaceCache,
		policy,
	)

	return &RedirectionInterceptor{
		currentClusterName: clusterMetadata.GetCurrentClusterName(),
		config:             configuration,
		redirectionPolicy:  dcRedirectionPolicy,
		namespaceCache:     namespaceCache,
		logger:             logger,
		clientBean:         clientBean,
		metricsHandler:     metricsHandler,
		timeSource:         timeSource,
	}
}

var _ grpc.UnaryServerInterceptor = (*RedirectionInterceptor)(nil).Intercept

func (i *RedirectionInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (_ any, retError error) {
	defer log.CapturePanic(i.logger, &retError)

	if _, isWorkflowHandler := info.Server.(*WorkflowHandler); !isWorkflowHandler {
		return handler(ctx, req)
	}
	if !i.redirectionAllowed(ctx) {
		return handler(ctx, req)
	}

	_, methodName := interceptor.SplitMethodName(info.FullMethod)
	if _, ok := localAPIResponses[methodName]; ok {
		return i.handleLocalAPIInvocation(ctx, req, handler, methodName)
	}
	if raFn, ok := globalAPIResponses[methodName]; ok {
		namespaceName, err := interceptor.GetNamespaceName(i.namespaceCache, req)
		if err != nil {
			return nil, err
		}
		return i.handleRedirectAPIInvocation(ctx, req, info, handler, methodName, raFn, namespaceName)
	}

	// This should not happen unless new API is added without updating localAPIResponses and  globalAPIResponses maps.
	// Also covered by unit test.
	i.logger.Warn("RedirectionInterceptor encountered unknown API", tag.Name(info.FullMethod))
	return handler(ctx, req)
}

func (i *RedirectionInterceptor) handleLocalAPIInvocation(
	ctx context.Context,
	req any,
	handler grpc.UnaryHandler,
	methodName string,
) (_ any, retError error) {
	scope, startTime := i.beforeCall(dcRedirectionMetricsPrefix + methodName)
	defer func() {
		i.afterCall(scope, startTime, i.currentClusterName, retError)
	}()
	return handler(ctx, req)
}

func (i *RedirectionInterceptor) handleRedirectAPIInvocation(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
	methodName string,
	respCtorFn responseConstructorFn,
	namespaceName namespace.Name,
) (_ any, retError error) {
	var resp any
	var clusterName string
	var err error

	scope, startTime := i.beforeCall(dcRedirectionMetricsPrefix + methodName)
	defer func() {
		i.afterCall(scope, startTime, clusterName, retError)
	}()

	err = i.redirectionPolicy.WithNamespaceRedirect(ctx, namespaceName, methodName, func(targetDC string) error {
		clusterName = targetDC
		if targetDC == i.currentClusterName {
			resp, err = handler(ctx, req)
		} else {
			remoteClient, _, err := i.clientBean.GetRemoteFrontendClient(targetDC)
			if err != nil {
				return err
			}
			resp = respCtorFn()
			err = remoteClient.Invoke(ctx, info.FullMethod, req, resp)
			if err != nil {
				return err
			}
		}
		return err
	})
	return resp, err
}

func (i *RedirectionInterceptor) beforeCall(
	operation string,
) (metrics.Handler, time.Time) {
	return i.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.ServiceRoleTag(metrics.DCRedirectionRoleTagValue)), i.timeSource.Now()
}

func (i *RedirectionInterceptor) afterCall(
	metricsHandler metrics.Handler,
	startTime time.Time,
	clusterName string,
	retError error,
) {
	metricsHandler = metricsHandler.WithTags(metrics.TargetClusterTag(clusterName))
	metricsHandler.Counter(metrics.ClientRedirectionRequests.GetMetricName()).Record(1)
	metricsHandler.Timer(metrics.ClientRedirectionLatency.GetMetricName()).Record(i.timeSource.Now().Sub(startTime))
	if retError != nil {
		metricsHandler.Counter(metrics.ClientRedirectionFailures.GetMetricName()).Record(1)
	}
}

func (i *RedirectionInterceptor) redirectionAllowed(
	ctx context.Context,
) bool {
	// default to allow dc redirection
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return true
	}
	values := md.Get(dcRedirectionContextHeaderName)
	if len(values) == 0 {
		return true
	}
	allowed, err := strconv.ParseBool(values[0])
	if err != nil {
		return true
	}
	return allowed
}
