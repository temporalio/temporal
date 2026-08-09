package scheduler_test

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const downloadedHistoryDirectoryEnv = "SCHEDULE_V1_HISTORY_DIR"

type observedStart struct {
	workflowID string
	runID      string
}

type scheduledWatch struct {
	request *schedulespb.WatchWorkflowRequest
}

type observedWatchCompletion struct {
	request  *schedulespb.WatchWorkflowRequest
	response *schedulespb.WatchWorkflowResponse
}

type localActivityMarkerMetadata struct {
	ActivityID   string
	ActivityType string
}

type v1HistoryTrace struct {
	args         *schedulespb.StartScheduleArgs
	history      *historypb.History
	starts       []observedStart
	watches      map[int64]scheduledWatch
	localWatches map[int64]observedWatchCompletion
	startTime    time.Time
	expectedSpec *schedulepb.Schedule
	searchAttrs  *commonpb.SearchAttributes
	memo         *commonpb.Memo
	baseActions  int64
	capturedIDs  bool
}

func TestDownloadedV1HistoriesAgainstCHASM(t *testing.T) {
	directory := os.Getenv(downloadedHistoryDirectoryEnv)
	if directory == "" {
		t.Skipf("%s is not set", downloadedHistoryDirectoryEnv)
	}
	paths, err := filepath.Glob(filepath.Join(directory, "*.json.gz"))
	require.NoError(t, err)
	nestedPaths, err := filepath.Glob(filepath.Join(directory, "*", "*.json.gz"))
	require.NoError(t, err)
	paths = append(paths, nestedPaths...)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			history := readReplayHistory(t, path)
			replayV1HistoryAgainstCHASM(t, history)
		})
	}
}

func TestV1HistoryAgainstCHASM_TimeSkippingStart(t *testing.T) {
	startTime := time.Unix(100, 0).UTC()
	args := &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(10 * time.Second),
			}}},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "type"},
				},
			}},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
			State:    &schedulepb.ScheduleState{},
		},
		Info: &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{
			Namespace: "namespace", NamespaceId: "namespace-id", ScheduleId: "schedule-id",
			LastProcessedTime: timestamppb.New(startTime),
		},
	}
	history := &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventId: 1, EventTime: timestamppb.New(startTime), EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: mustPayloads(t, args)},
			},
		},
		{
			EventId: 2, EventTime: timestamppb.New(startTime.Add(11 * time.Second)), EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
			Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{}},
		},
		{
			EventId: 3, EventTime: timestamppb.New(startTime.Add(11 * time.Second)), EventType: enumspb.EVENT_TYPE_MARKER_RECORDED,
			Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				MarkerName: "LocalActivity",
				Details: map[string]*commonpb.Payloads{
					"data":   mustPayloads(t, struct{ ActivityType string }{ActivityType: "StartWorkflow"}),
					"result": mustPayloads(t, &schedulespb.StartWorkflowResponse{RunId: "observed-run-id"}),
				},
			}},
		},
	}}

	replayV1HistoryAgainstCHASM(t, history)
}

func TestCaptureV1LocalActivities(t *testing.T) {
	history := readReplayHistory(t, filepath.Join(
		"..", "..", "..", "service", "worker", "scheduler", "testdata", "replay_v1.21.3.json.gz",
	))
	capture := captureV1LocalActivities(t, history)
	require.Len(t, capture.startsByActivityID, 10)
	require.Len(t, capture.watchesByActivityID, 9)
	for _, request := range capture.startsByActivityID {
		require.NotEmpty(t, request.GetRequest().GetWorkflowId())
	}
	for _, request := range capture.watchesByActivityID {
		require.NotEmpty(t, request.GetExecution().GetWorkflowId())
	}
	trace := extractV1HistoryTrace(t, history)
	require.Len(t, trace.localWatches, 9)
	for _, start := range trace.starts {
		require.NotEmpty(t, start.workflowID)
	}
}

func TestNormalizeScheduleForComparison(t *testing.T) {
	expected := &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}}
	actual := &schedulepb.Schedule{
		Spec:     &schedulepb.ScheduleSpec{},
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{},
	}
	protorequire.ProtoEqual(t, normalizeScheduleForComparison(expected), normalizeScheduleForComparison(actual))
}

func TestResolveObservedWorkflowID(t *testing.T) {
	workflowIDMap := map[string]string{"v1-workflow": "chasm-workflow"}

	workflowID, err := resolveObservedWorkflowID(workflowIDMap, true, "v1-workflow")
	require.NoError(t, err)
	require.Equal(t, "chasm-workflow", workflowID)

	workflowID, err = resolveObservedWorkflowID(workflowIDMap, false, "unmapped-workflow")
	require.NoError(t, err)
	require.Equal(t, "unmapped-workflow", workflowID)

	_, err = resolveObservedWorkflowID(workflowIDMap, true, "not-started")
	require.EqualError(t, err,
		`V1 workflow "not-started" completed before CHASM emitted its corresponding start; this is a scheduling/timing divergence`,
	)
}

func mustPayloads(t *testing.T, values ...any) *commonpb.Payloads {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	require.NoError(t, err)
	return payloads
}

func replayV1HistoryAgainstCHASM(t *testing.T, history *historypb.History) {
	t.Helper()
	trace := extractV1HistoryTrace(t, history)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	ctrl := gomock.NewController(t)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)

	var startMu sync.Mutex
	startIndex := 0
	workflowIDMap := make(map[string]string)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			startMu.Lock()
			defer startMu.Unlock()
			if startIndex >= len(trace.starts) {
				return nil, errors.New("CHASM emitted more workflow starts than V1 history")
			}
			start := trace.starts[startIndex]
			if start.workflowID != "" {
				workflowIDMap[start.workflowID] = request.GetWorkflowId()
			}
			result := &workflowservice.StartWorkflowExecutionResponse{RunId: start.runID}
			startIndex++
			return result, nil
		})

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newHistoryReplayLibrary(logger, frontendClient, historyClient)))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(trace.startTime)
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: trace.args.GetState().GetNamespaceId(),
		BusinessID:  trace.args.GetState().GetScheduleId(),
	})

	handler := scheduler.NewTestHandler(logger)
	initialInfo := trace.args.GetInfo()
	if initialInfo == nil {
		initialInfo = &schedulepb.ScheduleInfo{}
	}
	migrationRequest := migration.LegacyToCreateFromMigrationStateRequest(
		trace.args.GetSchedule(),
		initialInfo,
		trace.args.GetState(),
		trace.searchAttrs,
		trace.memo,
		trace.startTime,
	)
	for _, start := range migrationRequest.GetState().GetInvokerState().GetBufferedStarts() {
		if start.GetRunId() != "" {
			start.HasCallback = true
		}
	}
	_, err := handler.TestCreateFromMigrationState(engineCtx, migrationRequest)
	require.NoError(t, err)
	drainCHASMTasks(t, engine, rootRef, trace.startTime)

	for _, event := range trace.history.GetEvents()[1:] {
		now := event.GetEventTime().AsTime()
		timeSource.Update(now)
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			applyV1Signal(engineCtx, t, handler, trace, event)
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			_, err := engine.FirePureTasks(rootRef, now)
			require.NoError(t, err)
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			applyObservedCompletion(engineCtx, t, rootRef, trace, workflowIDMap, event)
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			if completion, ok := trace.localWatches[event.GetEventId()]; ok {
				applyObservedWatchCompletion(
					engineCtx, t, rootRef, workflowIDMap, trace.capturedIDs, completion.request, completion.response,
				)
			}
		default:
		}
		drainCHASMTasks(t, engine, rootRef, now)
	}

	startMu.Lock()
	observedStartCount := startIndex
	startMu.Unlock()
	require.Equal(t, len(trace.starts), observedStartCount, "CHASM workflow-start decisions differ from V1 history")
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (struct{}, error) {
			protorequire.ProtoEqual(
				t,
				normalizeScheduleForComparison(trace.expectedSpec),
				normalizeScheduleForComparison(s.GetSchedule()),
			)
			require.Equal(t, trace.baseActions+int64(len(trace.starts)), s.GetInfo().GetActionCount())
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
}

func normalizeScheduleForComparison(schedule *schedulepb.Schedule) *schedulepb.Schedule {
	normalized := proto.CloneOf(schedule)
	if proto.Equal(normalized.GetPolicies(), &schedulepb.SchedulePolicies{}) {
		normalized.Policies = nil
	}
	if proto.Equal(normalized.GetState(), &schedulepb.ScheduleState{}) {
		normalized.State = nil
	}
	return normalized
}

func newHistoryReplayLibrary(
	logger log.Logger,
	frontendClient workflowservice.WorkflowServiceClient,
	historyClient *historyservicemock.MockHistoryServiceClient,
) *scheduler.Library {
	config := defaultConfig()
	specProcessor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	invokerOptions := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		HistoryClient:  historyClient,
		FrontendClient: frontendClient,
	}
	return scheduler.NewLibrary(
		config,
		nil,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config: config, HistoryClient: historyClient, FrontendClient: frontendClient,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger,
			SpecProcessor: specProcessor, SpecBuilder: newLegacySpecBuilder(0, 0),
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOptions),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOptions),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, SpecProcessor: specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: logger, HistoryClient: historyClient,
		}),
	)
}

func drainCHASMTasks(t *testing.T, engine *chasmtest.Engine, rootRef chasm.ComponentRef, now time.Time) {
	t.Helper()
	for range 1000 {
		pure, err := engine.FirePureTasks(rootRef, now)
		require.NoError(t, err)
		sideEffect, err := engine.FireSideEffectTasks(rootRef, now)
		require.NoError(t, err)
		if pure+sideEffect == 0 {
			return
		}
	}
	require.FailNow(t, "CHASM task drain did not converge")
}

func applyV1Signal(
	ctx context.Context,
	t *testing.T,
	handler interface {
		UpdateSchedule(context.Context, *schedulerpb.UpdateScheduleRequest) (*schedulerpb.UpdateScheduleResponse, error)
		PatchSchedule(context.Context, *schedulerpb.PatchScheduleRequest) (*schedulerpb.PatchScheduleResponse, error)
	},
	trace *v1HistoryTrace,
	event *historypb.HistoryEvent,
) {
	t.Helper()
	attributes := event.GetWorkflowExecutionSignaledEventAttributes()
	switch attributes.GetSignalName() {
	case legacyscheduler.SignalNameUpdate:
		var update schedulespb.FullUpdateRequest
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &update))
		trace.expectedSpec = proto.CloneOf(update.GetSchedule())
		_, err := handler.UpdateSchedule(ctx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: trace.args.GetState().GetNamespaceId(),
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:        trace.args.GetState().GetNamespace(),
				ScheduleId:       trace.args.GetState().GetScheduleId(),
				Schedule:         proto.CloneOf(update.GetSchedule()),
				SearchAttributes: proto.CloneOf(update.GetSearchAttributes()),
			},
		})
		require.NoError(t, err)
	case legacyscheduler.SignalNamePatch:
		var patch schedulepb.SchedulePatch
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &patch))
		applyExpectedPatch(trace.expectedSpec, &patch)
		_, err := handler.PatchSchedule(ctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: trace.args.GetState().GetNamespaceId(),
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace: trace.args.GetState().GetNamespace(), ScheduleId: trace.args.GetState().GetScheduleId(), Patch: &patch,
			},
		})
		require.NoError(t, err)
	case legacyscheduler.SignalNameRefresh, legacyscheduler.SignalNameForceCAN:
	case legacyscheduler.SignalNameMigrateToChasm:
		require.FailNow(t, "history contains a live migration signal")
	default:
		require.FailNow(t, "unsupported V1 scheduler signal", attributes.GetSignalName())
	}
}

func applyExpectedPatch(schedule *schedulepb.Schedule, patch *schedulepb.SchedulePatch) {
	if patch.GetPause() != "" {
		schedule.GetState().Paused = true
		schedule.GetState().Notes = patch.GetPause()
	}
	if patch.GetUnpause() != "" {
		schedule.GetState().Paused = false
		schedule.GetState().Notes = patch.GetUnpause()
	}
}

func applyObservedCompletion(
	ctx context.Context,
	t *testing.T,
	rootRef chasm.ComponentRef,
	trace *v1HistoryTrace,
	workflowIDMap map[string]string,
	event *historypb.HistoryEvent,
) {
	t.Helper()
	attributes := event.GetActivityTaskCompletedEventAttributes()
	watch, ok := trace.watches[attributes.GetScheduledEventId()]
	if !ok {
		return
	}
	var response schedulespb.WatchWorkflowResponse
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &response))
	if response.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return
	}
	applyObservedWatchCompletion(ctx, t, rootRef, workflowIDMap, trace.capturedIDs, watch.request, &response)
}

func applyObservedWatchCompletion(
	ctx context.Context,
	t *testing.T,
	rootRef chasm.ComponentRef,
	workflowIDMap map[string]string,
	strictWorkflowMapping bool,
	request *schedulespb.WatchWorkflowRequest,
	response *schedulespb.WatchWorkflowResponse,
) {
	t.Helper()
	if response.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return
	}
	observedWorkflowID := request.GetExecution().GetWorkflowId()
	workflowID, err := resolveObservedWorkflowID(workflowIDMap, strictWorkflowMapping, observedWorkflowID)
	require.NoError(t, err)
	if err != nil {
		return
	}
	_, _, err = chasm.UpdateComponent(ctx, rootRef,
		func(s *scheduler.Scheduler, mutableCtx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker := s.Invoker.Get(mutableCtx)
			var requestID string
			for _, start := range invoker.GetBufferedStarts() {
				if start.GetWorkflowId() == workflowID && start.GetCompleted() == nil {
					requestID = start.GetRequestId()
					break
				}
			}
			if requestID == "" {
				return struct{}{}, fmt.Errorf("running workflow %q (V1 workflow %q) not found in CHASM", workflowID, observedWorkflowID)
			}
			return struct{}{}, s.HandleNexusCompletion(mutableCtx, nexusCompletion(response, requestID))
		}, struct{}{})
	require.NoError(t, err)
}

func resolveObservedWorkflowID(workflowIDMap map[string]string, strict bool, observedWorkflowID string) (string, error) {
	workflowID, mapped := workflowIDMap[observedWorkflowID]
	if mapped {
		return workflowID, nil
	}
	if !strict {
		return observedWorkflowID, nil
	}
	return "", fmt.Errorf(
		"V1 workflow %q completed before CHASM emitted its corresponding start; this is a scheduling/timing divergence",
		observedWorkflowID,
	)
}

func nexusCompletion(response *schedulespb.WatchWorkflowResponse, requestID string) *persistencespb.ChasmNexusCompletion {
	completion := &persistencespb.ChasmNexusCompletion{
		RequestId: requestID,
		CloseTime: response.GetCloseTime(),
	}
	if response.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		var result *commonpb.Payload
		if len(response.GetResult().GetPayloads()) != 0 {
			result = response.GetResult().GetPayloads()[0]
		}
		completion.Outcome = &persistencespb.ChasmNexusCompletion_Success{Success: result}
		return completion
	}
	failure := proto.CloneOf(response.GetFailure())
	if failure == nil {
		failure = &failurepb.Failure{}
	}
	switch response.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		failure.FailureInfo = &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		failure.FailureInfo = &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{}}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		failure.FailureInfo = &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}}
	default:
	}
	completion.Outcome = &persistencespb.ChasmNexusCompletion_Failure{Failure: failure}
	return completion
}

func extractV1HistoryTrace(t *testing.T, history *historypb.History) *v1HistoryTrace {
	t.Helper()
	require.NotEmpty(t, history.GetEvents())
	started := history.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	require.NotNil(t, started)
	var args schedulespb.StartScheduleArgs
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &args))
	trace := &v1HistoryTrace{
		args:         &args,
		history:      history,
		watches:      make(map[int64]scheduledWatch),
		localWatches: make(map[int64]observedWatchCompletion),
		startTime:    history.GetEvents()[0].GetEventTime().AsTime(),
		expectedSpec: proto.CloneOf(args.GetSchedule()),
		searchAttrs:  started.GetSearchAttributes(),
		memo:         started.GetMemo(),
		baseActions:  args.GetInfo().GetActionCount(),
	}
	localActivities := captureV1LocalActivities(t, history)
	trace.capturedIDs = localActivities != nil
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			metadata := localActivityMetadata(t, event)
			if metadata.ActivityType == "StartWorkflow" {
				var response schedulespb.StartWorkflowResponse
				details := event.GetMarkerRecordedEventAttributes().GetDetails()
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(details["result"], &response))
				start := observedStart{runID: response.GetRunId()}
				if localActivities != nil {
					request, ok := localActivities.startsByActivityID[metadata.ActivityID]
					require.True(t, ok, "local StartWorkflow marker %q has no matching V1 invocation", metadata.ActivityID)
					start.workflowID = request.GetRequest().GetWorkflowId()
				}
				trace.starts = append(trace.starts, start)
			}
			if metadata.ActivityType == "WatchWorkflow" {
				require.NotNil(t, localActivities)
				request, ok := localActivities.watchesByActivityID[metadata.ActivityID]
				require.True(t, ok, "local WatchWorkflow marker %q has no matching V1 invocation", metadata.ActivityID)
				var response schedulespb.WatchWorkflowResponse
				details := event.GetMarkerRecordedEventAttributes().GetDetails()
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(details["result"], &response))
				trace.localWatches[event.GetEventId()] = observedWatchCompletion{
					request:  request,
					response: &response,
				}
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			attributes := event.GetActivityTaskScheduledEventAttributes()
			if attributes.GetActivityType().GetName() == "WatchWorkflow" {
				var request schedulespb.WatchWorkflowRequest
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &request))
				trace.watches[event.GetEventId()] = scheduledWatch{request: &request}
			}
		default:
		}
	}
	return trace
}

type localActivityCapture struct {
	interceptor.WorkerInterceptorBase

	nextActivityID      int
	startsByActivityID  map[string]*schedulespb.StartWorkflowRequest
	watchesByActivityID map[string]*schedulespb.WatchWorkflowRequest
}

func (c *localActivityCapture) InterceptWorkflow(
	_ workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &localActivityCaptureInbound{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		capture:                        c,
	}
}

type localActivityCaptureInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	capture *localActivityCapture
}

func (i *localActivityCaptureInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&localActivityCaptureOutbound{
		WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound},
		capture:                         i.capture,
	})
}

type localActivityCaptureOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	capture *localActivityCapture
}

func (o *localActivityCaptureOutbound) ExecuteLocalActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	// Default local activity IDs follow invocation order; the fixture test verifies this
	// association against the IDs persisted in LocalActivity markers.
	o.capture.nextActivityID++
	activityID := fmt.Sprint(o.capture.nextActivityID)
	if len(args) == 1 {
		switch request := args[0].(type) {
		case *schedulespb.StartWorkflowRequest:
			if activityType == "StartWorkflow" {
				o.capture.startsByActivityID[activityID] = proto.CloneOf(request)
			}
		case *schedulespb.WatchWorkflowRequest:
			if activityType == "WatchWorkflow" {
				o.capture.watchesByActivityID[activityID] = proto.CloneOf(request)
			}
		default:
		}
	}
	return o.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func captureV1LocalActivities(t *testing.T, history *historypb.History) *localActivityCapture {
	t.Helper()
	if !historyHasLocalWatchMarkers(t, history) {
		return nil
	}
	capture := &localActivityCapture{
		startsByActivityID:  make(map[string]*schedulespb.StartWorkflowRequest),
		watchesByActivityID: make(map[string]*schedulespb.WatchWorkflowRequest),
	}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		DataConverter: converter.GetDefaultDataConverter(),
		Interceptors:  []interceptor.WorkerInterceptor{capture},
	})
	require.NoError(t, err)
	replayer.RegisterWorkflowWithOptions(
		legacyscheduler.SchedulerWorkflow,
		workflow.RegisterOptions{Name: legacyscheduler.WorkflowType},
	)
	require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), proto.CloneOf(history)))
	return capture
}

func historyHasLocalWatchMarkers(t *testing.T, history *historypb.History) bool {
	t.Helper()
	for _, event := range history.GetEvents() {
		if event.GetEventType() == enumspb.EVENT_TYPE_MARKER_RECORDED && localActivityMetadata(t, event).ActivityType == "WatchWorkflow" {
			return true
		}
	}
	return false
}

func localActivityMetadata(t *testing.T, event *historypb.HistoryEvent) localActivityMarkerMetadata {
	t.Helper()
	attributes := event.GetMarkerRecordedEventAttributes()
	if attributes.GetMarkerName() != "LocalActivity" {
		return localActivityMarkerMetadata{}
	}
	var metadata localActivityMarkerMetadata
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetDetails()["data"], &metadata))
	return metadata
}

func readReplayHistory(t *testing.T, path string) *historypb.History {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	history, err := client.HistoryFromJSON(reader, client.HistoryJSONOptions{})
	require.NoError(t, err)
	return history
}
