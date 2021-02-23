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

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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
		metricsClient metrics.Client
		scopes        map[string]int
		logger        log.Logger
	}
)

var _ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept

func NewTelemetryInterceptor(
	metricsClient metrics.Client,
	scopes map[string]int,
	logger log.Logger,
) *TelemetryInterceptor {
	return &TelemetryInterceptor{
		metricsClient: metricsClient,
		scopes:        scopes,
		logger:        logger,
	}
}

func (ti *TelemetryInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := splitMethodName(info.FullMethod)
	// if the method name is not defined, will default to
	// unknown scope, which carries value 0
	scope, _ := ti.scopes[methodName]
	var metricsScope metrics.Scope
	if namespace := GetNamespace(req); namespace != "" {
		metricsScope = ti.metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespace))
	} else {
		metricsScope = ti.metricsClient.Scope(scope).Tagged(metrics.NamespaceUnknownTag())
	}
	ctx = context.WithValue(ctx, metricsCtxKey, metricsScope)
	metricsScope.IncCounter(metrics.ServiceRequests)
	timer := metricsScope.StartTimer(metrics.ServiceLatency)
	defer timer.Stop()

	resp, err := handler(ctx, req)
	if err != nil {
		ti.handleError(scope, err)
		return nil, err
	}
	return resp, nil
}

func (ti *TelemetryInterceptor) handleError(
	scope int,
	err error,
) {

	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
		return
	}

	switch err := err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerrors.TaskAlreadyStarted:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrTaskAlreadyStartedCounter)
	case *serviceerror.InvalidArgument:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.NamespaceNotActive:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrNotFoundCounter)
	case *serviceerror.ResourceExhausted:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerrors.RetryReplication:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.NamespaceAlreadyExists:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrNamespaceAlreadyExistsCounter)
	case *serviceerror.QueryFailed:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrQueryFailedCounter)
	case *serviceerror.ClientVersionNotSupported:
		ti.metricsClient.IncCounter(scope, metrics.ServiceErrClientVersionNotSupportedCounter)
	case *serviceerror.DataLoss:
		ti.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		ti.logger.Error("internal service error, data loss", tag.Error(err))
	case *serviceerror.Internal:
		ti.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		ti.logger.Error("internal service error", tag.Error(err))
	default:
		ti.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		ti.logger.Error("uncategorized error", tag.Error(err))
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
