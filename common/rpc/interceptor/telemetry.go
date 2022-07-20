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
		metricsClient     metrics.Client
		scopes            map[string]int
		logger            log.Logger
	}
)

var (
	metricsCtxKey = metricsContextKey{}

	_ grpc.UnaryServerInterceptor = (*TelemetryInterceptor)(nil).Intercept
)

// static variables used to emit action metrics.
var (
	respondWorkflowTaskCompleted = "RespondWorkflowTaskCompleted"
	pollActivityTaskQueue        = "PollActivityTaskQueue"

	grpcActions = map[string]int{
		"QueryWorkflow":                    metrics.FrontendQueryWorkflowScope,
		"RecordActivityTaskHeartbeat":      metrics.FrontendRecordActivityTaskHeartbeatScope,
		"RecordActivityTaskHeartbeatById":  metrics.FrontendRecordActivityTaskHeartbeatByIdScope,
		"ResetWorkflowExecution":           metrics.FrontendResetWorkflowExecutionScope,
		"StartWorkflowExecution":           metrics.FrontendStartWorkflowExecutionScope,
		"SignalWorkflowExecution":          metrics.FrontendSignalWorkflowExecutionScope,
		"SignalWithStartWorkflowExecution": metrics.FrontendSignalWithStartWorkflowExecutionScope,
		respondWorkflowTaskCompleted:       metrics.FrontendRespondWorkflowTaskCompletedScope,
		pollActivityTaskQueue:              metrics.FrontendPollActivityTaskQueueScope,
	}

	commandActions = map[enums.CommandType]struct{}{
		enums.COMMAND_TYPE_RECORD_MARKER:                      {},
		enums.COMMAND_TYPE_START_TIMER:                        {},
		enums.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:             {},
		enums.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:     {},
		enums.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION: {},
		enums.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:  {},
		enums.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION: {},
	}
)

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
	// remove this "if" case when that is done.
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

	// emit action metrics only after successful calls
	ti.emitActionMetric(methodName, req, metricsScope, resp)

	return resp, nil
}

func (ti *TelemetryInterceptor) emitActionMetric(
	methodName string,
	req interface{},
	scope metrics.Scope,
	result interface{},
) {
	if actionScope, ok := grpcActions[methodName]; !ok || actionScope != ti.scopes[methodName] {
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
					scope.Tagged(metrics.ActionType("command_RecordMarker_" + markerName)).IncCounter(metrics.ActionCounter)
				default:
					// handle all other command action
					scope.Tagged(metrics.ActionType("command_" + command.CommandType.String())).IncCounter(metrics.ActionCounter)
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
			scope.Tagged(metrics.ActionType("activity_retry")).IncCounter(metrics.ActionCounter)
		}

	default:
		// grpc action
		scope.Tagged(metrics.ActionType("grpc_" + methodName)).IncCounter(metrics.ActionCounter)
	}
}

func (ti *TelemetryInterceptor) metricsScopeLogTags(
	req interface{},
	methodName string,
) (metrics.Scope, []tag.Tag) {

	// if the method name is not defined, will default to
	// unknown scope, which carries value 0
	scopeDef := ti.scopes[methodName]
	scopeDef = ti.overrideScope(scopeDef, req)

	nsName := GetNamespace(ti.namespaceRegistry, req)
	if nsName == "" {
		return ti.metricsClient.Scope(scopeDef).Tagged(metrics.NamespaceUnknownTag()), []tag.Tag{tag.Operation(methodName)}
	}
	return ti.metricsClient.Scope(scopeDef).Tagged(metrics.NamespaceTag(nsName.String())), []tag.Tag{
		tag.Operation(methodName),
		tag.WorkflowNamespace(nsName.String()),
	}
}

func (ti *TelemetryInterceptor) handleError(
	scope metrics.Scope,
	logTags []tag.Tag,
	err error,
) {

	scope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.ServiceErrorWithType)

	if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
		return
	}

	switch err := err.(type) {
	// we emit service_error_with_type metrics, no need to emit specific metric for these known error types.
	case *serviceerror.WorkflowNotReady,
		*serviceerror.NamespaceInvalidState,
		*serviceerror.InvalidArgument,
		*serviceerror.NamespaceNotActive,
		*serviceerror.WorkflowExecutionAlreadyStarted,
		*serviceerror.NotFound,
		*serviceerror.NamespaceNotFound,
		*serviceerror.NamespaceAlreadyExists,
		*serviceerror.QueryFailed,
		*serviceerror.ClientVersionNotSupported,
		*serviceerror.PermissionDenied,
		*serviceerrors.StickyWorkerUnavailable,
		*serviceerrors.ShardOwnershipLost,
		*serviceerrors.TaskAlreadyStarted,
		*serviceerrors.RetryReplication:
		// no-op

	// specific metric for resource exhausted error with throttle reason
	case *serviceerror.ResourceExhausted:
		scope.Tagged(metrics.ResourceExhaustedCauseTag(err.Cause)).IncCounter(metrics.ServiceErrResourceExhaustedCounter)

	// Any other errors are treated as ServiceFailures against SLA.
	// Including below known errors and any other unknown errors.
	//  *serviceerror.DataLoss,
	//	*serviceerror.Unavailable:
	default:
		scope.IncCounter(metrics.ServiceFailures)
		ti.logger.Error("service failures", append(logTags, tag.Error(err))...)
	}
}

func MetricsScope(
	ctx context.Context,
	logger log.Logger,
) metrics.Scope {
	scope, ok := ctx.Value(metricsCtxKey).(metrics.Scope)
	if !ok {
		logger.Error("unable to get metrics scope")
		return metrics.NoopScope
	}
	return scope
}
