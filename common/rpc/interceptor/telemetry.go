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
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"
)

type (
	metricsContextKey struct{}

	TelemetryInterceptor struct {
		namespaceRegistry namespace.Registry
		serializer        common.TaskTokenSerializer
		metricsHandler    metrics.Handler
		logger            log.Logger
		logAllReqErrors   dynamicconfig.BoolPropertyFnWithNamespaceFilter
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
		enums.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:           {},
		enums.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION:     {},
	}
)

func NewTelemetryInterceptor(
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
	logAllReqErrors dynamicconfig.BoolPropertyFnWithNamespaceFilter,
) *TelemetryInterceptor {
	return &TelemetryInterceptor{
		namespaceRegistry: namespaceRegistry,
		serializer:        common.NewProtoTaskTokenSerializer(),
		metricsHandler:    metricsHandler,
		logger:            logger,
		logAllReqErrors:   logAllReqErrors,
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
			if request, ok := req.(*workflowservice.GetWorkflowExecutionHistoryRequest); ok {
				if request.GetWaitNewEvent() {
					return metrics.FrontendPollWorkflowExecutionHistoryScope
				}
			}
		}
		return operation
	} else if strings.HasPrefix(fullName, api.HistoryServicePrefix) {
		// GetWorkflowExecutionHistory method handles both long poll and regular calls.
		// Current plan is to eventually split GetWorkflowExecutionHistory into two APIs,
		// remove this "if" case when that is done.
		if operation == metrics.HistoryGetWorkflowExecutionHistoryScope {
			if request, ok := req.(*historyservice.GetWorkflowExecutionHistoryRequest); ok {
				if r := request.GetRequest(); r != nil && r.GetWaitNewEvent() {
					return metrics.HistoryPollWorkflowExecutionHistoryScope
				}
			}
		}
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
	nsName := MustGetNamespaceName(ti.namespaceRegistry, req)

	metricsHandler, logTags := ti.unaryMetricsHandlerLogTags(req, info.FullMethod, methodName, nsName)

	ctx = AddTelemetryContext(ctx, metricsHandler)
	metrics.ServiceRequests.With(metricsHandler).Record(1)

	startTime := time.Now().UTC()
	defer func() {
		ti.RecordLatencyMetrics(ctx, startTime, metricsHandler)
	}()

	resp, err := handler(ctx, req)

	if err != nil {
		ti.HandleError(req, metricsHandler, logTags, err, nsName)
	} else {
		// emit action metrics only after successful calls
		ti.emitActionMetric(methodName, info.FullMethod, req, metricsHandler, resp)
	}

	return resp, err
}

func AddTelemetryContext(ctx context.Context, metricsHandler metrics.Handler) context.Context {
	return context.WithValue(ctx, metricsCtxKey, metricsHandler)
}

func (ti *TelemetryInterceptor) RecordLatencyMetrics(ctx context.Context, startTime time.Time, metricsHandler metrics.Handler) {
	userLatencyDuration := time.Duration(0)
	if val, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name()); ok {
		userLatencyDuration = time.Duration(val)
		metrics.ServiceLatencyUserLatency.With(metricsHandler).Record(userLatencyDuration)
	}

	latency := time.Since(startTime)
	metrics.ServiceLatency.With(metricsHandler).Record(latency)
	noUserLatency := max(0, latency-userLatencyDuration)
	metrics.ServiceLatencyNoUserLatency.With(metricsHandler).Record(noUserLatency)
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
		ti.HandleError(nil, metricsHandler, logTags, err, "")
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

func (ti *TelemetryInterceptor) unaryMetricsHandlerLogTags(req interface{},
	fullMethod string,
	methodName string,
	nsName namespace.Name) (metrics.Handler, []tag.Tag) {
	overridedMethodName := ti.unaryOverrideOperationTag(fullMethod, methodName, req)

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

func (ti *TelemetryInterceptor) HandleError(
	req interface{},
	metricsHandler metrics.Handler,
	logTags []tag.Tag,
	err error,
	nsName namespace.Name) {
	statusCode := serviceerror.ToStatus(err).Code()

	recordMetrics(metricsHandler, err, statusCode)

	ti.logErrors(req, nsName, err, statusCode, logTags)
}

func (ti *TelemetryInterceptor) logErrors(
	req interface{},
	nsName namespace.Name,
	err error,
	statusCode codes.Code,
	logTags []tag.Tag) {
	logAllErrors := nsName != "" && ti.logAllReqErrors(nsName.String())
	if !logAllErrors && (common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err)) {
		return
	}

	if !logAllErrors && isUserCaused(statusCode) {
		return
	}

	// We mask these two error types in MaskInternalErrorDetailsInterceptor, so we need the hash to find the actual
	// error message.
	if statusCode == codes.Internal || statusCode == codes.Unknown {
		errorHash := common.ErrorHash(err)
		logTags = append(logTags, tag.NewStringTag("hash", errorHash))
	}

	logTags = append(logTags, tag.NewStringTag("grpc_code", statusCode.String()))
	logTags = append(logTags, ti.getWorkflowTags(req)...)

	ti.logger.Error("service failures", append(logTags, tag.Error(err))...)
}

func isUserCaused(statusCode codes.Code) bool {
	switch statusCode {
	case codes.InvalidArgument,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.NotFound:
		return true
	case codes.OK,
		codes.Canceled,
		codes.Unknown,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss:
		return false
	}

	return false
}

func recordMetrics(metricsHandler metrics.Handler, err error, statusCode codes.Code) {
	metrics.ServiceErrorWithType.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))

	switch err := err.(type) {
	case *serviceerror.ResourceExhausted:
		metrics.ServiceErrResourceExhaustedCounter.With(metricsHandler).Record(
			1, metrics.ResourceExhaustedCauseTag(err.Cause), metrics.ResourceExhaustedScopeTag(err.Scope))
		return
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
		return
	}

	if !isUserCaused(statusCode) {
		metrics.ServiceFailures.With(metricsHandler).Record(1)
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
		logger.Info("unable to get metrics scope")
		return metrics.NoopMetricsHandler
	}
	return handler
}
