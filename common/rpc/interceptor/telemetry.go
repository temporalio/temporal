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

package interceptor

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	metricsContextKey struct{}
)

var (
	metricsCtxKey = metricsContextKey{}
)

type (
	TelemetryInterceptor struct {
		namespaceRegistry namespace.Registry
		metricsClient     metrics.Client
		scopes            map[string]int
		logger            log.Logger
	}
)

var _ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept

func NewTelemetryInterceptor(
	namespaceRegistry namespace.Registry,
	metricsClient metrics.Client,
	scopes map[string]int,
	logger log.Logger,
) *TelemetryInterceptor {
	return &TelemetryInterceptor{
		namespaceRegistry: namespaceRegistry,
		metricsClient:     metricsClient,
		scopes:            scopes,
		logger:            logger,
	}
}

// Use this method to override scope used for reporting a metric.
// Ideally this method should never be used.
func (ti *TelemetryInterceptor) overrideScope(scope int, req interface{}) int {
	// GetWorkflowExecutionHistory method handles both long poll and regular calls.
	// Current plan is to eventually split GetWorkflowExecutionHistory into two APIs,
	// remove this if case when that is done.
	if scope == metrics.FrontendGetWorkflowExecutionHistoryScope {
		request := req.(*workflowservice.GetWorkflowExecutionHistoryRequest)
		if request.GetWaitNewEvent() {
			return metrics.FrontendPollWorkflowExecutionHistoryScope
		}
	}
	return scope
}

func (ti *TelemetryInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	metricsScope, logTags := ti.metricsScopeLogTags(req, methodName)

	ctx = context.WithValue(ctx, metricsCtxKey, metricsScope)
	metricsScope.IncCounter(metrics.ServiceRequests)

	timer := metricsScope.StartTimer(metrics.ServiceLatency)
	defer timer.Stop()
	timerNoUserLatency := metricsScope.StartTimer(metrics.ServiceLatencyNoUserLatency)
	defer timerNoUserLatency.Stop()

	resp, err := handler(ctx, req)

	if val, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency); ok {
		userLatencyDuration := time.Duration(val)
		timerNoUserLatency.Subtract(userLatencyDuration)
		metricsScope.RecordTimer(metrics.ServiceLatencyUserLatency, userLatencyDuration)
	}

	if err != nil {
		ti.handleError(metricsScope, logTags, err)
		return nil, err
	}

	return resp, nil
}

func (ti *TelemetryInterceptor) metricsScopeLogTags(
	req interface{},
	methodName string,
) (metrics.Scope, []tag.Tag) {

	// if the method name is not defined, will default to
	// unknown scope, which carries value 0
	scopeDef, _ := ti.scopes[methodName]
	scopeDef = ti.overrideScope(scopeDef, req)

	namespace := GetNamespace(ti.namespaceRegistry, req)
	if namespace == "" {
		return ti.metricsClient.Scope(scopeDef).Tagged(metrics.NamespaceUnknownTag()), []tag.Tag{tag.Operation(methodName)}
	}
	return ti.metricsClient.Scope(scopeDef).Tagged(metrics.NamespaceTag(namespace.String())), []tag.Tag{
		tag.Operation(methodName),
		tag.WorkflowNamespace(namespace.String()),
	}
}

func (ti *TelemetryInterceptor) handleError(
	scope metrics.Scope,
	logTags []tag.Tag,
	err error,
) {

	if common.IsContextDeadlineExceededErr(err) {
		scope.IncCounter(metrics.ServiceErrContextTimeoutCounter)
		return
	}
	if common.IsContextCanceledErr(err) {
		scope.IncCounter(metrics.ServiceErrContextCancelledCounter)
		return
	}

	switch err := err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		scope.IncCounter(metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerrors.TaskAlreadyStarted:
		scope.IncCounter(metrics.ServiceErrTaskAlreadyStartedCounter)
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.NamespaceNotActive:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		scope.IncCounter(metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.ServiceErrNotFoundCounter)
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerrors.RetryReplication:
		scope.IncCounter(metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.NamespaceAlreadyExists:
		scope.IncCounter(metrics.ServiceErrNamespaceAlreadyExistsCounter)
	case *serviceerror.QueryFailed:
		scope.IncCounter(metrics.ServiceErrQueryFailedCounter)
	case *serviceerror.ClientVersionNotSupported:
		scope.IncCounter(metrics.ServiceErrClientVersionNotSupportedCounter)
	case *serviceerror.DataLoss:
		scope.IncCounter(metrics.ServiceFailures)
		ti.logger.Error("unavailable error, data loss", append(logTags, tag.Error(err))...)
	case *serviceerror.Unavailable:
		scope.IncCounter(metrics.ServiceFailures)
		ti.logger.Error("unavailable error", append(logTags, tag.Error(err))...)
	default:
		scope.IncCounter(metrics.ServiceFailures)
		ti.logger.Error("uncategorized error", append(logTags, tag.Error(err))...)
	}
}

func MetricsScope(
	ctx context.Context,
	logger log.Logger,
) metrics.Scope {
	scope, ok := ctx.Value(metricsCtxKey).(metrics.Scope)
	if !ok {
		logger.Error("unable to get metrics scope")
		return metrics.NoopScope(metrics.Frontend)
	}
	return scope
}
