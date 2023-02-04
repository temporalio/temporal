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
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor"
)

var (
	dcRedirectionMetricsPrefix = "DCRedirection"

	localAPIResults = map[string]respAllocFn{
		// Namespace APIs, namespace APIs does not require redirection
		"DeprecateNamespace": func() interface{} { return new(workflowservice.DeprecateNamespaceResponse) },
		"DescribeNamespace":  func() interface{} { return new(workflowservice.DescribeNamespaceResponse) },
		"ListNamespaces":     func() interface{} { return new(workflowservice.ListNamespacesResponse) },
		"RegisterNamespace":  func() interface{} { return new(workflowservice.RegisterNamespaceResponse) },
		"UpdateNamespace":    func() interface{} { return new(workflowservice.UpdateNamespaceResponse) },

		// Cluster info APIs, Cluster info APIs does not require redirection
		"GetSearchAttributes": func() interface{} { return new(workflowservice.GetSearchAttributesResponse) },
		"GetClusterInfo":      func() interface{} { return new(workflowservice.GetClusterInfoResponse) },
		"GetSystemInfo":       func() interface{} { return new(workflowservice.GetSystemInfoResponse) },
	}

	globalAPIResults = map[string]respAllocFn{
		"DescribeTaskQueue":                  func() interface{} { return new(workflowservice.DescribeTaskQueueResponse) },
		"DescribeWorkflowExecution":          func() interface{} { return new(workflowservice.DescribeWorkflowExecutionResponse) },
		"GetWorkflowExecutionHistory":        func() interface{} { return new(workflowservice.GetWorkflowExecutionHistoryResponse) },
		"GetWorkflowExecutionHistoryReverse": func() interface{} { return new(workflowservice.GetWorkflowExecutionHistoryReverseResponse) },
		"ListArchivedWorkflowExecutions":     func() interface{} { return new(workflowservice.ListArchivedWorkflowExecutionsResponse) },
		"ListClosedWorkflowExecutions":       func() interface{} { return new(workflowservice.ListClosedWorkflowExecutionsResponse) },
		"ListOpenWorkflowExecutions":         func() interface{} { return new(workflowservice.ListOpenWorkflowExecutionsResponse) },
		"ListWorkflowExecutions":             func() interface{} { return new(workflowservice.ListWorkflowExecutionsResponse) },
		"ScanWorkflowExecutions":             func() interface{} { return new(workflowservice.ScanWorkflowExecutionsResponse) },
		"CountWorkflowExecutions":            func() interface{} { return new(workflowservice.CountWorkflowExecutionsResponse) },
		"PollActivityTaskQueue":              func() interface{} { return new(workflowservice.PollActivityTaskQueueResponse) },
		"PollWorkflowTaskQueue":              func() interface{} { return new(workflowservice.PollWorkflowTaskQueueResponse) },
		"QueryWorkflow":                      func() interface{} { return new(workflowservice.QueryWorkflowResponse) },
		"RecordActivityTaskHeartbeat":        func() interface{} { return new(workflowservice.RecordActivityTaskHeartbeatResponse) },
		"RecordActivityTaskHeartbeatById":    func() interface{} { return new(workflowservice.RecordActivityTaskHeartbeatByIdResponse) },
		"RequestCancelWorkflowExecution":     func() interface{} { return new(workflowservice.RequestCancelWorkflowExecutionResponse) },
		"ResetStickyTaskQueue":               func() interface{} { return new(workflowservice.ResetStickyTaskQueueResponse) },
		"ResetWorkflowExecution":             func() interface{} { return new(workflowservice.ResetWorkflowExecutionResponse) },
		"RespondActivityTaskCanceled":        func() interface{} { return new(workflowservice.RespondActivityTaskCanceledResponse) },
		"RespondActivityTaskCanceledById":    func() interface{} { return new(workflowservice.RespondActivityTaskCanceledByIdResponse) },
		"RespondActivityTaskCompleted":       func() interface{} { return new(workflowservice.RespondActivityTaskCompletedResponse) },
		"RespondActivityTaskCompletedById":   func() interface{} { return new(workflowservice.RespondActivityTaskCompletedByIdResponse) },
		"RespondActivityTaskFailed":          func() interface{} { return new(workflowservice.RespondActivityTaskFailedResponse) },
		"RespondActivityTaskFailedById":      func() interface{} { return new(workflowservice.RespondActivityTaskFailedByIdResponse) },
		"RespondWorkflowTaskCompleted":       func() interface{} { return new(workflowservice.RespondWorkflowTaskCompletedResponse) },
		"RespondWorkflowTaskFailed":          func() interface{} { return new(workflowservice.RespondWorkflowTaskFailedResponse) },
		"RespondQueryTaskCompleted":          func() interface{} { return new(workflowservice.RespondQueryTaskCompletedResponse) },
		"SignalWithStartWorkflowExecution":   func() interface{} { return new(workflowservice.SignalWithStartWorkflowExecutionResponse) },
		"SignalWorkflowExecution":            func() interface{} { return new(workflowservice.SignalWorkflowExecutionResponse) },
		"StartWorkflowExecution":             func() interface{} { return new(workflowservice.StartWorkflowExecutionResponse) },
		"UpdateWorkflowExecution":            func() interface{} { return new(workflowservice.UpdateWorkflowExecutionResponse) },
		"TerminateWorkflowExecution":         func() interface{} { return new(workflowservice.TerminateWorkflowExecutionResponse) },
		"DeleteWorkflowExecution":            func() interface{} { return new(workflowservice.DeleteWorkflowExecutionResponse) },
		"ListTaskQueuePartitions":            func() interface{} { return new(workflowservice.ListTaskQueuePartitionsResponse) },

		"CreateSchedule":              func() interface{} { return new(workflowservice.CreateScheduleResponse) },
		"DescribeSchedule":            func() interface{} { return new(workflowservice.DescribeScheduleResponse) },
		"UpdateSchedule":              func() interface{} { return new(workflowservice.UpdateScheduleResponse) },
		"PatchSchedule":               func() interface{} { return new(workflowservice.PatchScheduleResponse) },
		"DeleteSchedule":              func() interface{} { return new(workflowservice.DeleteScheduleResponse) },
		"ListSchedules":               func() interface{} { return new(workflowservice.ListSchedulesResponse) },
		"ListScheduleMatchingTimes":   func() interface{} { return new(workflowservice.ListScheduleMatchingTimesResponse) },
		"UpdateWorkerBuildIdOrdering": func() interface{} { return new(workflowservice.UpdateWorkerBuildIdOrderingResponse) },
		"GetWorkerBuildIdOrdering":    func() interface{} { return new(workflowservice.GetWorkerBuildIdOrderingResponse) },

		"StartBatchOperation":    func() interface{} { return new(workflowservice.StartBatchOperationResponse) },
		"StopBatchOperation":     func() interface{} { return new(workflowservice.StopBatchOperationResponse) },
		"DescribeBatchOperation": func() interface{} { return new(workflowservice.DescribeBatchOperationResponse) },
		"ListBatchOperations":    func() interface{} { return new(workflowservice.ListBatchOperationsResponse) },
	}
)

type (
	respAllocFn func() interface{}

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
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (_ interface{}, retError error) {
	defer log.CapturePanic(i.logger, &retError)

	_, methodName := interceptor.SplitMethodName(info.FullMethod)
	if _, ok := localAPIResults[methodName]; ok {
		return i.handleLocalAPIInvocation(ctx, req, handler, methodName)
	}
	if respAllocFn, ok := globalAPIResults[methodName]; ok {
		namespaceName := interceptor.GetNamespace(i.namespaceCache, req)
		return i.handleRedirectAPIInvocation(ctx, req, info, handler, methodName, respAllocFn, namespaceName)
	}

	// this should not happen, log warn and move on
	i.logger.Warn(fmt.Sprintf("RedirectionInterceptor encountered unknown API: %v", methodName))
	return handler(ctx, req)
}

func (i *RedirectionInterceptor) handleLocalAPIInvocation(
	ctx context.Context,
	req interface{},
	handler grpc.UnaryHandler,
	methodName string,
) (_ interface{}, retError error) {
	scope, startTime := i.beforeCall(dcRedirectionMetricsPrefix + methodName)
	defer func() {
		i.afterCall(scope, startTime, i.currentClusterName, retError)
	}()
	return handler(ctx, req)
}

func (i *RedirectionInterceptor) handleRedirectAPIInvocation(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
	methodName string,
	respAllocFn func() interface{},
	namespaceName namespace.Name,
) (_ interface{}, retError error) {
	var resp interface{}
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
			resp = respAllocFn()
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
