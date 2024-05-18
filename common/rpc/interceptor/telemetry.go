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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
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
		serializer        common.TaskTokenSerializer
		metricsHandler    metrics.Handler
		logger            log.Logger
	}
	ExecutionGetter interface {
		GetExecution() *commonpb.WorkflowExecution
	}
	WorkflowExecutionGetter interface {
		GetWorkflowExecution() *commonpb.WorkflowExecution
	}
	WorkflowIdGetter interface {
		GetWorkflowId() string
	}
	RunIdGetter interface {
		GetRunId() string
	}
)

var (
	metricsCtxKey = metricsContextKey{}

	updateAcceptanceMessageBody anypb.Any
	_                           = updateAcceptanceMessageBody.MarshalFrom(&updatepb.Acceptance{})

	updateRejectionMessageBody anypb.Any
	_                          = updateRejectionMessageBody.MarshalFrom(&updatepb.Rejection{})

	updateResponseMessageBody anypb.Any
	_                         = updateResponseMessageBody.MarshalFrom(&updatepb.Response{})

	_ grpc.UnaryServerInterceptor  = (*TelemetryInterceptor)(nil).UnaryIntercept
	_ grpc.StreamServerInterceptor = (*TelemetryInterceptor)(nil).StreamIntercept
)

var (
	respondWorkflowTaskCompleted = "RespondWorkflowTaskCompleted"
	pollActivityTaskQueue        = "PollActivityTaskQueue"
	startWorkflowExecution       = "StartWorkflowExecution"

	grpcActions = map[string]struct{}{
		startWorkflowExecution:             {},
		respondWorkflowTaskCompleted:       {},
		pollActivityTaskQueue:              {},
		"QueryWorkflow":                    {},
		"RecordActivityTaskHeartbeat":      {},
		"RecordActivityTaskHeartbeatById":  {},
		"ResetWorkflowExecution":           {},
		"SignalWorkflowExecution":          {},
		"SignalWithStartWorkflowExecution": {},
		"CreateSchedule":                   {},
		"UpdateSchedule":                   {},
		"DeleteSchedule":                   {},
		"PatchSchedule":                    {},
	}

	// commandActions is a subset of all the commands that are counted as actions.
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
		serializer:        common.NewProtoTaskTokenSerializer(),
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

// Use this method to override scope used for reporting a metric.
// Ideally this method should never be used.
func (ti *TelemetryInterceptor) unaryOverrideOperationTag(fullName, operation string, req interface{}) string {
	if strings.HasPrefix(fullName, api.WorkflowServicePrefix) {
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
	if strings.HasPrefix(fullName, api.OperatorServicePrefix) {
		return "Operator" + operation
	}
	// prepend Admin prefix to Admin APIs
	if strings.HasPrefix(fullName, api.AdminServicePrefix) {
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
	methodName := api.MethodName(info.FullMethod)
	metricsHandler, logTags := ti.unaryMetricsHandlerLogTags(req, info.FullMethod, methodName)

	ctx = context.WithValue(ctx, metricsCtxKey, metricsHandler)
	metrics.ServiceRequests.With(metricsHandler).Record(1)

	startTime := time.Now().UTC()
	userLatencyDuration := time.Duration(0)
	defer func() {
		latency := time.Since(startTime)
		metrics.ServiceLatency.With(metricsHandler).Record(latency)
		noUserLatency := latency - userLatencyDuration
		if noUserLatency < 0 {
			noUserLatency = 0
		}
		metrics.ServiceLatencyNoUserLatency.With(metricsHandler).Record(noUserLatency)
	}()

	resp, err := handler(ctx, req)

	if val, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name()); ok {
		userLatencyDuration = time.Duration(val)
		startTime.Add(userLatencyDuration)
		metrics.ServiceLatencyUserLatency.With(metricsHandler).Record(userLatencyDuration)
	}

	if err != nil {
		ti.handleError(req, metricsHandler, logTags, err)
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
	methodName := api.MethodName(info.FullMethod)
	metricsHandler, logTags := ti.streamMetricsHandlerLogTags(info.FullMethod, methodName)
	metrics.ServiceRequests.With(metricsHandler).Record(1)

	err := handler(service, serverStream)
	if err != nil {
		ti.handleError(nil, metricsHandler, logTags, err)
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
	if _, ok := grpcActions[methodName]; !ok || !strings.HasPrefix(fullName, api.WorkflowServicePrefix) {
		// grpcActions checks that methodName is the one that we care about, and we only care about WorkflowService.
		return
	}

	switch methodName {
	case startWorkflowExecution:
		resp, ok := result.(*workflowservice.StartWorkflowExecutionResponse)
		if !ok {
			return
		}
		if resp.Started {
			metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName))
		}

	case respondWorkflowTaskCompleted:
		// handle commands
		completedRequest, ok := req.(*workflowservice.RespondWorkflowTaskCompletedRequest)
		if !ok {
			return
		}

		hasMarker := false
		for _, command := range completedRequest.Commands {
			if _, ok := commandActions[command.CommandType]; !ok {
				continue
			}

			switch command.CommandType { // nolint:exhaustive
			case enums.COMMAND_TYPE_RECORD_MARKER:
				// handle RecordMarker command, they are used for localActivity, sideEffect, versioning etc.
				hasMarker = true
			case enums.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
				// Each child workflow counts as 2 actions. We use separate tags to track them separately.
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("command_"+command.CommandType.String()))
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("command_"+command.CommandType.String()+"_Extra"))
			default:
				// handle all other command action
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("command_"+command.CommandType.String()))
			}
		}

		if hasMarker {
			// Emit separate action metric for batch of markers.
			// One workflow task response may contain multiple marker commands. Each marker will emit one
			// command_RecordMarker_Xxx action metric. Depending on pricing model, you may want to ignore all individual
			// command_RecordMarker_Xxx and use command_BatchMarkers instead.
			metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("command_BatchMarkers"))
		}

		for _, msg := range completedRequest.Messages {
			if msg == nil || msg.Body == nil {
				continue
			}
			switch msg.Body.GetTypeUrl() {
			case updateAcceptanceMessageBody.TypeUrl:
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("message_UpdateWorkflowExecution:Acceptance"))
			case updateRejectionMessageBody.TypeUrl:
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("message_UpdateWorkflowExecution:Rejection"))
			case updateResponseMessageBody.TypeUrl:
				// not billed
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
			metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("activity_retry"))
		}

	default:
		// grpc action
		metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName))
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
	req interface{},
	metricsHandler metrics.Handler,
	logTags []tag.Tag,
	err error,
) {

	metrics.ServiceErrorWithType.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))

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
		metrics.ServiceErrResourceExhaustedCounter.With(metricsHandler).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
	// Any other errors are treated as ServiceFailures against SLA unless constructed with the standard
	// `status.Error` (or Errorf) constructors, in which case the status code is checked below.
	// Including below known errors and any other unknown errors.
	//  *serviceerror.DataLoss,
	//  *serviceerror.Internal
	//	*serviceerror.Unavailable:
	default:
		// Also skip emitting ServiceFailures for non serviceerrors returned from handlers for certain error
		// codes.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument,
				codes.AlreadyExists,
				codes.FailedPrecondition,
				codes.OutOfRange,
				codes.PermissionDenied,
				codes.Unauthenticated,
				codes.NotFound:
				return
			}
		}
		metrics.ServiceFailures.With(metricsHandler).Record(1)
		logTags = append(logTags, ti.getWorkflowTags(req)...)
		ti.logger.Error("service failures", append(logTags, tag.Error(err))...)
	}
}

func (ti *TelemetryInterceptor) getWorkflowTags(
	req interface{},
) []tag.Tag {
	if executionGetter, ok := req.(ExecutionGetter); ok {
		execution := executionGetter.GetExecution()
		return []tag.Tag{tag.WorkflowID(execution.WorkflowId), tag.WorkflowRunID(execution.RunId)}
	}
	if workflowExecutionGetter, ok := req.(WorkflowExecutionGetter); ok {
		execution := workflowExecutionGetter.GetWorkflowExecution()
		return []tag.Tag{tag.WorkflowID(execution.WorkflowId), tag.WorkflowRunID(execution.RunId)}
	}
	if taskTokenGetter, ok := req.(TaskTokenGetter); ok {
		taskTokenBytes := taskTokenGetter.GetTaskToken()
		if len(taskTokenBytes) == 0 {
			return []tag.Tag{}
		}
		// Special case for avoiding deprecated RespondQueryTaskCompleted API token which does not have workflow id.
		if _, ok := req.(*workflowservice.RespondQueryTaskCompletedRequest); ok {
			return []tag.Tag{}
		}
		taskToken, err := ti.serializer.Deserialize(taskTokenBytes)
		if err != nil {
			ti.logger.Error("unable to deserialize task token", tag.Error(err))
			return []tag.Tag{}
		}
		return []tag.Tag{tag.WorkflowID(taskToken.WorkflowId), tag.WorkflowRunID(taskToken.RunId)}
	}
	if workflowIdGetter, ok := req.(WorkflowIdGetter); ok {
		workflowTags := []tag.Tag{tag.WorkflowID(workflowIdGetter.GetWorkflowId())}
		if runIdGetter, ok := req.(RunIdGetter); ok {
			workflowTags = append(workflowTags, tag.WorkflowRunID(runIdGetter.GetRunId()))
		}
		return workflowTags
	}
	return []tag.Tag{}
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
