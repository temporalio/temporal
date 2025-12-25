package interceptor

import (
	"context"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/frontend/configs"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type (
	metricsContextKey struct{}

	TelemetryInterceptor struct {
		namespaceRegistry   namespace.Registry
		metricsHandler      metrics.Handler
		logger              log.Logger
		workflowTags        *logtags.WorkflowTags
		logAllReqErrors     dynamicconfig.BoolPropertyFnWithNamespaceFilter
		requestErrorHandler ErrorHandler
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
	respondWorkflowTaskCompleted   = "RespondWorkflowTaskCompleted"
	pollActivityTaskQueue          = "PollActivityTaskQueue"
	startWorkflowExecution         = "StartWorkflowExecution"
	executeMultiOperation          = "ExecuteMultiOperation"
	queryWorkflow                  = "QueryWorkflow"
	updateWorkflowExecutionOptions = "UpdateWorkflowExecutionOptions"

	grpcActions = map[string]struct{}{
		startWorkflowExecution:             {},
		executeMultiOperation:              {},
		respondWorkflowTaskCompleted:       {},
		pollActivityTaskQueue:              {},
		queryWorkflow:                      {},
		updateWorkflowExecutionOptions:     {},
		"RecordActivityTaskHeartbeat":      {},
		"RecordActivityTaskHeartbeatById":  {},
		"ResetWorkflowExecution":           {},
		"SignalWorkflowExecution":          {},
		"SignalWithStartWorkflowExecution": {},
		"CreateSchedule":                   {},
		"UpdateSchedule":                   {},
		"DeleteSchedule":                   {},
		"PatchSchedule":                    {},
		"PauseWorkflowExecution":           {},
		"UnpauseWorkflowExecution":         {},
	}

	// commandActions is a subset of all the commands that are counted as actions.
	commandActions = map[enumspb.CommandType]struct{}{
		enumspb.COMMAND_TYPE_RECORD_MARKER:                      {},
		enumspb.COMMAND_TYPE_START_TIMER:                        {},
		enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:             {},
		enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:     {},
		enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION: {},
		enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:  {},
		enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:         {},
		enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION: {},
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:           {},
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION:     {},
	}
)

func NewTelemetryInterceptor(
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
	logAllReqErrors dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	requestErrorHandler ErrorHandler,
) *TelemetryInterceptor {
	return &TelemetryInterceptor{
		namespaceRegistry:   namespaceRegistry,
		metricsHandler:      metricsHandler,
		logger:              logger,
		workflowTags:        logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		logAllReqErrors:     logAllReqErrors,
		requestErrorHandler: requestErrorHandler,
	}
}

// telemetryUnaryOverrideOperationTag is used to override scope used for reporting a metric.
// Ideally this method should never be used.
func telemetryUnaryOverrideOperationTag(fullName, operation string, req any) string {
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
	return telemetryOverrideOperationTag(fullName, operation)
}

// telemetryOverrideOperationTag is used to override scope used for reporting a metric.
// Ideally this method should never be used.
func telemetryOverrideOperationTag(fullName, operation string) string {
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
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
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

	if configs.IsAPIOperation(info.FullMethod) {
		metrics.OperationCounter.With(metricsHandler).Record(
			1,
			metrics.TaskTypeTag(""), // Added to make tags consistent with history task executor.
		)
	}

	if err != nil {
		ti.requestErrorHandler.HandleError(req, info.FullMethod, metricsHandler, logTags, err, nsName)
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
	service any,
	serverStream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	methodName := api.MethodName(info.FullMethod)
	metricsHandler, logTags := ti.streamMetricsHandlerLogTags(info.FullMethod, methodName)
	metrics.ServiceRequests.With(metricsHandler).Record(1)

	err := handler(service, serverStream)
	if err != nil {
		ti.requestErrorHandler.HandleError(nil, info.FullMethod, metricsHandler, logTags, err, "")
		return err
	}
	return nil
}

func (ti *TelemetryInterceptor) emitActionMetric(
	methodName string,
	fullName string,
	req any,
	metricsHandler metrics.Handler,
	result any,
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
		} else {
			typedReq, ok := req.(*workflowservice.StartWorkflowExecutionRequest)
			if ok && typedReq.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING && typedReq.GetOnConflictOptions() != nil {
				metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName+"_UpdateWorkflowExecutionOptions"))
			}
		}
	case executeMultiOperation:
		resp, ok := result.(*workflowservice.ExecuteMultiOperationResponse)
		if !ok {
			return
		}
		if len(resp.Responses) > 0 {
			if startResp := resp.GetResponses()[0].GetStartWorkflow(); startResp != nil {
				if startResp.Started {
					metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName))
				} else {
					typedReq, ok := req.(*workflowservice.ExecuteMultiOperationRequest)
					if !ok || typedReq == nil || len(typedReq.Operations) == 0 {
						return
					}

					if typedReq.GetOperations()[0].GetStartWorkflow().GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING &&
						typedReq.GetOperations()[0].GetStartWorkflow().GetOnConflictOptions() != nil {
						metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName+"_UpdateWorkflowExecutionOptions"))
					}
				}
			}
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
			case enumspb.COMMAND_TYPE_RECORD_MARKER:
				// handle RecordMarker command, they are used for localActivity, sideEffect, versioning etc.
				hasMarker = true
			case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
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
	case queryWorkflow:
		queryWorkflowReq, ok := req.(*workflowservice.QueryWorkflowRequest)
		if !ok {
			return
		}
		queryType := queryWorkflowReq.GetQuery().GetQueryType()
		switch queryType {
		case "__temporal_workflow_metadata":
			return
		default:
			metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName))
		}
	default:
		// grpc action
		metrics.ActionCounter.With(metricsHandler).Record(1, metrics.ActionType("grpc_"+methodName))
	}
}

// CreateUnaryMetricsHandlerLogTags creates metrics handler and log tags for unary RPC calls
func CreateUnaryMetricsHandlerLogTags(
	baseMetricsHandler metrics.Handler,
	req any,
	fullMethod string,
	methodName string,
	nsName namespace.Name,
) (metrics.Handler, []tag.Tag) {
	overridedMethodName := telemetryUnaryOverrideOperationTag(fullMethod, methodName, req)

	if nsName == "" {
		return baseMetricsHandler.WithTags(metrics.OperationTag(overridedMethodName), metrics.NamespaceUnknownTag()),
			[]tag.Tag{tag.Operation(overridedMethodName)}
	}
	return baseMetricsHandler.WithTags(metrics.OperationTag(overridedMethodName), metrics.NamespaceTag(nsName.String())),
		[]tag.Tag{tag.Operation(overridedMethodName), tag.WorkflowNamespace(nsName.String())}
}

func (ti *TelemetryInterceptor) unaryMetricsHandlerLogTags(req any,
	fullMethod string,
	methodName string,
	nsName namespace.Name) (metrics.Handler, []tag.Tag) {
	return CreateUnaryMetricsHandlerLogTags(ti.metricsHandler, req, fullMethod, methodName, nsName)
}

func (ti *TelemetryInterceptor) streamMetricsHandlerLogTags(
	fullMethod string,
	methodName string,
) (metrics.Handler, []tag.Tag) {
	overridedMethodName := telemetryOverrideOperationTag(fullMethod, methodName)
	return ti.metricsHandler.WithTags(
		metrics.OperationTag(overridedMethodName),
		metrics.NamespaceUnknownTag(),
	), []tag.Tag{tag.Operation(overridedMethodName)}
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
