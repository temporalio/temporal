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
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
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

	TelemetryInterceptor struct {
		namespaceRegistry namespace.Registry
		metricsHandler    metrics.Handler
		logger            log.Logger
	}
)

var (
	metricsCtxKey = metricsContextKey{}

	_ grpc.UnaryServerInterceptor  = (*TelemetryInterceptor)(nil).UnaryIntercept
	_ grpc.StreamServerInterceptor = (*TelemetryInterceptor)(nil).StreamIntercept
)

// static variables used to emit action metrics.
var (
	respondWorkflowTaskCompleted = "RespondWorkflowTaskCompleted"
	pollActivityTaskQueue        = "PollActivityTaskQueue"
	frontendPackagePrefix        = "/temporal.api.workflowservice.v1.WorkflowService/"
	operatorServicePrefix        = "/temporal.api.operatorservice.v1.OperatorService/"
	adminServicePrefix           = "/temporal.server.api.adminservice.v1.AdminService/"

	grpcActions = map[string]struct{}{
		metrics.FrontendQueryWorkflowScope:                    {},
		metrics.FrontendRecordActivityTaskHeartbeatScope:      {},
		metrics.FrontendRecordActivityTaskHeartbeatByIdScope:  {},
		metrics.FrontendResetWorkflowExecutionScope:           {},
		metrics.FrontendStartWorkflowExecutionScope:           {},
		metrics.FrontendSignalWorkflowExecutionScope:          {},
		metrics.FrontendSignalWithStartWorkflowExecutionScope: {},
		metrics.FrontendRespondWorkflowTaskCompletedScope:     {},
		metrics.FrontendPollActivityTaskQueueScope:            {},
	}

	commandActions = map[enums.CommandType]struct{}{
		enums.COMMAND_TYPE_RECORD_MARKER:                      {},
		enums.COMMAND_TYPE_START_TIMER:                        {},
		enums.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:             {},
		enums.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:     {},
		enums.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION: {},
		enums.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:  {},
		enums.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:         {},
		enums.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION: {},
	}
)

func NewTelemetryInterceptor(
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *TelemetryInterceptor {
	return &TelemetryInterceptor{
		namespaceRegistry: namespaceRegistry,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

// Use this method to override scope used for reporting a metric.
// Ideally this method should never be used.
func (ti *TelemetryInterceptor) unaryOverrideOperationTag(fullName, operation string, req interface{}) string {
	if strings.HasPrefix(fullName, frontendPackagePrefix) {
		// GetWorkflowExecutionHistory method handles both long poll and regular calls.
		// Current plan is to eventually split GetWorkflowExecutionHistory into two APIs,
		// remove this "if" case when that is done.
		if operation == metrics.FrontendGetWorkflowExecutionHistoryScope {
			request := req.(*workflowservice.GetWorkflowExecutionHistoryRequest)
			if request.GetWaitNewEvent() {
				return metrics.FrontendPollWorkflowExecutionHistoryScope
			}
		}
		return operation
	}
	return ti.overrideOperationTag(fullName, operation)
}

// Use this method to override scope used for reporting a metric.
// Ideally this method should never be used.
func (ti *TelemetryInterceptor) overrideOperationTag(fullName, operation string) string {
	// prepend Operator prefix to Operator APIs
	if strings.HasPrefix(fullName, operatorServicePrefix) {
		return "Operator" + operation
	}
	// prepend Admin prefix to Admin APIs
	if strings.HasPrefix(fullName, adminServicePrefix) {
		return "Admin" + operation
	}
	return operation
}

func (ti *TelemetryInterceptor) UnaryIntercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	_, methodName := SplitMethodName(info.FullMethod)
	metricsHandler, logTags := ti.unaryMetricsHandlerLogTags(req, info.FullMethod, methodName)

	ctx = context.WithValue(ctx, metricsCtxKey, metricsHandler)
	metricsHandler.Counter(metrics.ServiceRequests.GetMetricName()).Record(1)

	startTime := time.Now().UTC()
	userLatencyDuration := time.Duration(0)
	defer func() {
		latency := time.Since(startTime)
		metricsHandler.Timer(metrics.ServiceLatency.GetMetricName()).Record(latency)
		noUserLatency := latency - userLatencyDuration
		if noUserLatency < 0 {
			noUserLatency = 0
		}
		metricsHandler.Timer(metrics.ServiceLatencyNoUserLatency.GetMetricName()).Record(noUserLatency)
	}()

	resp, err := handler(ctx, req)

	if val, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName()); ok {
		userLatencyDuration = time.Duration(val)
		startTime.Add(userLatencyDuration)
		metricsHandler.Timer(metrics.ServiceLatencyUserLatency.GetMetricName()).Record(userLatencyDuration)
	}

	if err != nil {
		ti.handleError(metricsHandler, logTags, err)
		return nil, err
	}

	// emit action metrics only after successful calls
	ti.emitActionMetric(methodName, info.FullMethod, req, metricsHandler, resp)

	return resp, nil
}

func (ti *TelemetryInterceptor) StreamIntercept(
	service interface{},
	serverStream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	_, methodName := SplitMethodName(info.FullMethod)
	metricsHandler, logTags := ti.streamMetricsHandlerLogTags(info.FullMethod, methodName)
	metricsHandler.Counter(metrics.ServiceRequests.GetMetricName()).Record(1)

	err := handler(service, serverStream)
	if err != nil {
		ti.handleError(metricsHandler, logTags, err)
		return err
	}
	return nil
}

func (ti *TelemetryInterceptor) emitActionMetric(
	methodName string,
	fullName string,
	req interface{},
	metricsHandler metrics.Handler,
	result interface{},
) {
	if _, ok := grpcActions[methodName]; !ok || !strings.HasPrefix(fullName, frontendPackagePrefix) {
		// grpcActions checks that methodName is the one that we care about.
		// ti.scopes verifies that the scope is the one we intended to emit action metrics.
		// This is necessary because TelemetryInterceptor is used for all services. Different service could have same
		// method name. But we only want to emit action metrics from frontend.
		return
	}

	switch methodName {
	case respondWorkflowTaskCompleted:
		// handle commands
		completedRequest, ok := req.(*workflowservice.RespondWorkflowTaskCompletedRequest)
		if !ok {
			return
		}

		for _, command := range completedRequest.Commands {
			if _, ok := commandActions[command.CommandType]; ok {
				switch command.CommandType {
				case enums.COMMAND_TYPE_RECORD_MARKER:
					// handle RecordMarker command, they are used for localActivity, sideEffect, versioning etc.
					markerName := command.GetRecordMarkerCommandAttributes().GetMarkerName()
					metricsHandler.Counter(metrics.ActionCounter.GetMetricName()).Record(1, metrics.ActionType("command_RecordMarker_"+markerName))
				default:
					// handle all other command action
					metricsHandler.Counter(metrics.ActionCounter.GetMetricName()).Record(1, metrics.ActionType("command_"+command.CommandType.String()))
				}
			}
		}

	case pollActivityTaskQueue:
		// handle activity retries
		activityPollResponse, ok := result.(*workflowservice.PollActivityTaskQueueResponse)
		if !ok {
			return
		}
		if activityPollResponse == nil || len(activityPollResponse.TaskToken) == 0 {
			// empty response
			return
		}
		if activityPollResponse.Attempt > 1 {
			metricsHandler.Counter(metrics.ActionCounter.GetMetricName()).Record(1, metrics.ActionType("activity_retry"))
		}

	default:
		// grpc action
		metricsHandler.Counter(metrics.ActionCounter.GetMetricName()).Record(1, metrics.ActionType("grpc_"+methodName))
	}
}

func (ti *TelemetryInterceptor) unaryMetricsHandlerLogTags(
	req interface{},
	fullMethod string,
	methodName string,
) (metrics.Handler, []tag.Tag) {
	overridedMethodName := ti.unaryOverrideOperationTag(fullMethod, methodName, req)

	nsName := MustGetNamespaceName(ti.namespaceRegistry, req)
	if nsName == "" {
		return ti.metricsHandler.WithTags(metrics.OperationTag(overridedMethodName), metrics.NamespaceUnknownTag()),
			[]tag.Tag{tag.Operation(overridedMethodName)}
	}
	return ti.metricsHandler.WithTags(metrics.OperationTag(overridedMethodName), metrics.NamespaceTag(nsName.String())),
		[]tag.Tag{tag.Operation(overridedMethodName), tag.WorkflowNamespace(nsName.String())}
}

func (ti *TelemetryInterceptor) streamMetricsHandlerLogTags(
	fullMethod string,
	methodName string,
) (metrics.Handler, []tag.Tag) {
	overridedMethodName := ti.overrideOperationTag(fullMethod, methodName)
	return ti.metricsHandler.WithTags(
		metrics.OperationTag(overridedMethodName),
		metrics.NamespaceUnknownTag(),
	), []tag.Tag{tag.Operation(overridedMethodName)}
}

func (ti *TelemetryInterceptor) handleError(
	metricsHandler metrics.Handler,
	logTags []tag.Tag,
	err error,
) {

	metricsHandler.Counter(metrics.ServiceErrorWithType.GetMetricName()).Record(1, metrics.ServiceErrorTypeTag(err))

	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		return
	}

	switch err := err.(type) {
	// we emit service_error_with_type metrics, no need to emit specific metric for these known error types.
	case *serviceerror.AlreadyExists,
		*serviceerror.CancellationAlreadyRequested,
		*serviceerror.FailedPrecondition,
		*serviceerror.NamespaceInvalidState,
		*serviceerror.NamespaceNotActive,
		*serviceerror.NamespaceNotFound,
		*serviceerror.NamespaceAlreadyExists,
		*serviceerror.InvalidArgument,
		*serviceerror.WorkflowExecutionAlreadyStarted,
		*serviceerror.WorkflowNotReady,
		*serviceerror.NotFound,
		*serviceerror.QueryFailed,
		*serviceerror.ClientVersionNotSupported,
		*serviceerror.ServerVersionNotSupported,
		*serviceerror.PermissionDenied,
		*serviceerror.NewerBuildExists,
		*serviceerrors.StickyWorkerUnavailable,
		*serviceerrors.ShardOwnershipLost,
		*serviceerrors.TaskAlreadyStarted,
		*serviceerrors.RetryReplication:
		// no-op

	// specific metric for resource exhausted error with throttle reason
	case *serviceerror.ResourceExhausted:
		metricsHandler.Counter(metrics.ServiceErrResourceExhaustedCounter.GetMetricName()).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))

	// Any other errors are treated as ServiceFailures against SLA.
	// Including below known errors and any other unknown errors.
	//  *serviceerror.DataLoss,
	//  *serviceerror.Internal
	//	*serviceerror.Unavailable:
	default:
		metricsHandler.Counter(metrics.ServiceFailures.GetMetricName()).Record(1)
		ti.logger.Error("service failures", append(logTags, tag.Error(err))...)
	}
}

func GetMetricsHandlerFromContext(
	ctx context.Context,
	logger log.Logger,
) metrics.Handler {
	handler, ok := ctx.Value(metricsCtxKey).(metrics.Handler)
	if !ok {
		logger.Error("unable to get metrics scope")
		return metrics.NoopMetricsHandler
	}
	return handler
}
