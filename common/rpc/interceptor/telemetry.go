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
	"fmt"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

func (i *TelemetryInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := SplitMethodName(info.FullMethod)
	scope, ok := i.scopes[methodName]
	if !ok {
		i.logger.Error(fmt.Sprintf("unable to find scope for %v", methodName))
	}
	i.metricsClient.IncCounter(scope, metrics.ServiceRequests)
	timer := i.metricsClient.StartTimer(scope, metrics.ServiceLatency)
	defer timer.Stop()

	resp, err := handler(ctx, req)
	if err != nil {
		i.handleError(scope, err)
		return nil, err
	}
	return resp, nil
}

func (i *TelemetryInterceptor) handleError(
	scope int,
	err error,
) {

	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		i.metricsClient.IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
		return
	}

	switch err := err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerrors.TaskAlreadyStarted:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrTaskAlreadyStartedCounter)
	case *serviceerror.InvalidArgument:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.NamespaceNotActive:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrNotFoundCounter)
	case *serviceerror.CancellationAlreadyRequested:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrCancellationAlreadyRequestedCounter)
	case *serviceerror.ResourceExhausted:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerrors.RetryReplication:
		i.metricsClient.IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.Internal:
		i.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		i.logger.Error("internal service error", tag.Error(err))
	default:
		i.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		i.logger.Error("uncategorized error", tag.Error(err))
	}
}
