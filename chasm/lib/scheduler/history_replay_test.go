package scheduler_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

const (
	downloadedHistoryDirectoryEnv = "SCHEDULE_V1_HISTORY_DIR"
	replayReportPathEnv           = "SCHEDULE_V2_REPLAY_REPORT"
	replayFailOnEnv               = "SCHEDULE_V2_REPLAY_FAIL_ON"
)

type replayClassification string

const (
	replayClassificationMatch        replayClassification = "match"
	replayClassificationTimingOnly   replayClassification = "timing_only"
	replayClassificationSignificant  replayClassification = "significant"
	replayClassificationUnsupported  replayClassification = "unsupported"
	replayClassificationInconclusive replayClassification = "inconclusive"
)

type replayDivergence struct {
	Classification replayClassification `json:"classification"`
	Kind           string               `json:"kind"`
	Message        string               `json:"message"`
	WorkflowID     string               `json:"workflowId,omitempty"`
	V1Time         *time.Time           `json:"v1Time,omitempty"`
	CHASMTime      *time.Time           `json:"chasmTime,omitempty"`
}

type replayCaseResult struct {
	History          string               `json:"history,omitempty"`
	Namespace        string               `json:"namespace"`
	ScheduleID       string               `json:"scheduleId"`
	Classification   replayClassification `json:"classification"`
	V1Starts         []string             `json:"v1Starts"`
	CHASMStarts      []string             `json:"chasmStarts"`
	V1ActionCount    int64                `json:"v1ActionCount"`
	CHASMActionCount int64                `json:"chasmActionCount"`
	CHASMState       replayStateSnapshot  `json:"chasmState"`
	Divergences      []replayDivergence   `json:"divergences,omitempty"`
}

type replayStateSnapshot struct {
	LastProcessedTime *time.Time `json:"lastProcessedTime,omitempty"`
	Paused            bool       `json:"paused"`
	BufferedStarts    []string   `json:"bufferedStarts,omitempty"`
}

type replayReport struct {
	Version int                `json:"version"`
	Summary map[string]int     `json:"summary"`
	Cases   []replayCaseResult `json:"cases"`
}

type observedStart struct {
	workflowID string
	runID      string
	time       time.Time
}

type chasmStart struct {
	workflowID string
	time       time.Time
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
	sort.Strings(paths)
	require.NotEmpty(t, paths)
	results := make([]replayCaseResult, 0, len(paths))
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			history := readReplayHistory(t, path)
			result := replayV1HistoryAgainstCHASM(t, history)
			result.History = path
			results = append(results, result)
			if replayResultFails(result, os.Getenv(replayFailOnEnv)) {
				t.Errorf("V1/CHASM replay classified as %s: %v", result.Classification, result.Divergences)
			}
		})
	}
	if reportPath := os.Getenv(replayReportPathEnv); reportPath != "" {
		require.NoError(t, writeReplayReport(reportPath, results))
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

	result := replayV1HistoryAgainstCHASM(t, history)
	require.Equal(t, replayClassificationMatch, result.Classification)
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

func TestReplayReport(t *testing.T) {
	results := []replayCaseResult{
		{ScheduleID: "matching", Classification: replayClassificationMatch},
		{ScheduleID: "different", Classification: replayClassificationSignificant},
	}
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, writeReplayReport(path, results))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var report replayReport
	require.NoError(t, json.Unmarshal(data, &report))
	require.Equal(t, 1, report.Version)
	require.Equal(t, map[string]int{"match": 1, "significant": 1}, report.Summary)
	require.Equal(t, results, report.Cases)

	require.True(t, replayResultFails(results[1], "significant"))
	require.False(t, replayResultFails(results[1], "none"))
	require.True(t, replayResultFails(replayCaseResult{Classification: replayClassificationTimingOnly}, "all"))
}

func mustPayloads(t *testing.T, values ...any) *commonpb.Payloads {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(values...)
	require.NoError(t, err)
	return payloads
}

func replayV1HistoryAgainstCHASM(t *testing.T, history *historypb.History) replayCaseResult {
	t.Helper()
	trace := extractV1HistoryTrace(t, history)
	result := replayCaseResult{
		Namespace:      trace.args.GetState().GetNamespace(),
		ScheduleID:     trace.args.GetState().GetScheduleId(),
		Classification: replayClassificationMatch,
	}
	for _, start := range trace.starts {
		result.V1Starts = append(result.V1Starts, start.workflowID)
	}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	ctrl := gomock.NewController(t)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)

	var startMu sync.Mutex
	startIndex := 0
	workflowIDMap := make(map[string]string)
	var chasmStarts []chasmStart
	var divergences []replayDivergence
	startsByWorkflowID := make(map[string]observedStart, len(trace.starts))
	for _, start := range trace.starts {
		if start.workflowID != "" {
			startsByWorkflowID[start.workflowID] = start
		}
	}
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(trace.startTime)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			startMu.Lock()
			defer startMu.Unlock()
			workflowID := request.GetWorkflowId()
			chasmTime := timeSource.Now().UTC()
			chasmStarts = append(chasmStarts, chasmStart{workflowID: workflowID, time: chasmTime})
			if start, ok := startsByWorkflowID[request.GetWorkflowId()]; ok {
				if _, duplicate := workflowIDMap[start.workflowID]; duplicate {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationSignificant,
						Kind:           "duplicate_action",
						Message:        fmt.Sprintf("CHASM emitted duplicate workflow start %q", workflowID),
						WorkflowID:     workflowID,
						CHASMTime:      timePointer(chasmTime),
					})
					return &workflowservice.StartWorkflowExecutionResponse{RunId: start.runID}, nil
				}
				workflowIDMap[start.workflowID] = request.GetWorkflowId()
				startIndex++
				if !start.time.IsZero() && !start.time.Equal(chasmTime) {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationTimingOnly,
						Kind:           "action_time",
						Message:        "V1 and CHASM emitted the same workflow start at different observed times",
						WorkflowID:     workflowID,
						V1Time:         timePointer(start.time),
						CHASMTime:      timePointer(chasmTime),
					})
				}
				return &workflowservice.StartWorkflowExecutionResponse{RunId: start.runID}, nil
			}
			if len(startsByWorkflowID) == 0 && startIndex < len(trace.starts) {
				start := trace.starts[startIndex]
				startIndex++
				return &workflowservice.StartWorkflowExecutionResponse{RunId: start.runID}, nil
			}
			divergences = append(divergences, replayDivergence{
				Classification: replayClassificationSignificant,
				Kind:           "extra_action",
				Message:        fmt.Sprintf("CHASM emitted workflow start %q not present in V1 history", workflowID),
				WorkflowID:     workflowID,
				CHASMTime:      timePointer(chasmTime),
			})
			return &workflowservice.StartWorkflowExecutionResponse{RunId: fmt.Sprintf("chasm-replay-extra-%d", len(chasmStarts))}, nil
		})

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newHistoryReplayLibrary(logger, frontendClient, historyClient)))
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
	if initialPatch := trace.args.GetInitialPatch(); initialPatch != nil {
		applyExpectedPatch(trace.expectedSpec, initialPatch)
		_, err = handler.PatchSchedule(engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: trace.args.GetState().GetNamespaceId(),
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  trace.args.GetState().GetNamespace(),
				ScheduleId: trace.args.GetState().GetScheduleId(),
				Patch:      proto.CloneOf(initialPatch),
			},
		})
		require.NoError(t, err)
	}
	drainCHASMTasks(t, engine, rootRef, trace.startTime)
	currentTime := trace.startTime
	var pendingCompletions []observedWatchCompletion
	applyPendingCompletions := func(now time.Time) {
		for {
			appliedAny := false
			remaining := pendingCompletions[:0]
			for _, completion := range pendingCompletions {
				applied, err := applyObservedWatchCompletion(
					engineCtx,
					rootRef,
					workflowIDMap,
					trace.capturedIDs,
					completion.request,
					completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationSignificant,
						Kind:           "completion_state",
						Message:        err.Error(),
						WorkflowID:     completion.request.GetExecution().GetWorkflowId(),
						CHASMTime:      timePointer(now),
					})
					applied = true
				}
				if applied {
					appliedAny = true
				} else {
					remaining = append(remaining, completion)
				}
			}
			pendingCompletions = remaining
			if !appliedAny {
				return
			}
			drainCHASMTasks(t, engine, rootRef, now)
		}
	}

	for _, event := range trace.history.GetEvents()[1:] {
		now := event.GetEventTime().AsTime()
		if now.Before(currentTime) {
			now = currentTime
		}
		advanceCHASMTime(
			t,
			engine,
			rootRef,
			timeSource,
			&currentTime,
			now,
			!isV1ExternalInput(trace, event),
			applyPendingCompletions,
		)
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			if err := applyV1Signal(engineCtx, handler, trace, event); err != nil {
				divergences = append(divergences, replayDivergence{
					Classification: replayClassificationUnsupported,
					Kind:           "external_input",
					Message:        err.Error(),
					V1Time:         timePointer(now),
				})
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			if completion, ok := observedActivityCompletion(t, trace, event); ok {
				applied, err := applyObservedWatchCompletion(
					engineCtx, rootRef, workflowIDMap, trace.capturedIDs, completion.request, completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationSignificant,
						Kind:           "completion_state",
						Message:        err.Error(),
						WorkflowID:     completion.request.GetExecution().GetWorkflowId(),
						V1Time:         timePointer(now),
					})
					applied = true
				}
				if !applied {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationTimingOnly,
						Kind:           "completion_before_start",
						Message:        "V1 observed workflow completion before CHASM emitted the corresponding start",
						WorkflowID:     completion.request.GetExecution().GetWorkflowId(),
						V1Time:         timePointer(now),
					})
					pendingCompletions = append(pendingCompletions, completion)
				}
			}
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			if completion, ok := trace.localWatches[event.GetEventId()]; ok {
				applied, err := applyObservedWatchCompletion(
					engineCtx, rootRef, workflowIDMap, trace.capturedIDs, completion.request, completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationSignificant,
						Kind:           "completion_state",
						Message:        err.Error(),
						WorkflowID:     completion.request.GetExecution().GetWorkflowId(),
						V1Time:         timePointer(now),
					})
					applied = true
				}
				if !applied {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationTimingOnly,
						Kind:           "completion_before_start",
						Message:        "V1 observed workflow completion before CHASM emitted the corresponding start",
						WorkflowID:     completion.request.GetExecution().GetWorkflowId(),
						V1Time:         timePointer(now),
					})
					pendingCompletions = append(pendingCompletions, completion)
				}
			}
		default:
		}
		drainCHASMTasks(t, engine, rootRef, now)
		applyPendingCompletions(now)
	}

	startMu.Lock()
	for _, start := range trace.starts {
		if start.workflowID != "" {
			if _, ok := workflowIDMap[start.workflowID]; !ok {
				divergences = append(divergences, replayDivergence{
					Classification: replayClassificationSignificant,
					Kind:           "missing_action",
					Message:        fmt.Sprintf("V1 emitted workflow start %q but CHASM did not", start.workflowID),
					WorkflowID:     start.workflowID,
					V1Time:         timePointer(start.time),
				})
			}
		}
	}
	if len(startsByWorkflowID) == 0 && startIndex != len(trace.starts) {
		divergences = append(divergences, replayDivergence{
			Classification: replayClassificationSignificant,
			Kind:           "action_count",
			Message:        fmt.Sprintf("V1 emitted %d workflow starts and CHASM emitted %d", len(trace.starts), startIndex),
		})
	}
	for _, start := range chasmStarts {
		result.CHASMStarts = append(result.CHASMStarts, start.workflowID)
	}
	startMu.Unlock()
	if len(pendingCompletions) != 0 {
		divergences = append(divergences, replayDivergence{
			Classification: replayClassificationInconclusive,
			Kind:           "unapplied_completion",
			Message:        fmt.Sprintf("%d V1 workflow completions remained beyond the replay horizon", len(pendingCompletions)),
		})
	}
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, chasmCtx chasm.Context, _ struct{}) (struct{}, error) {
			if !proto.Equal(
				normalizeScheduleForComparison(trace.expectedSpec),
				normalizeScheduleForComparison(s.GetSchedule()),
			) {
				divergences = append(divergences, replayDivergence{
					Classification: replayClassificationSignificant,
					Kind:           "schedule_state",
					Message:        "final normalized schedule state differs",
				})
			}
			expectedActionCount := trace.baseActions + int64(len(trace.starts))
			result.V1ActionCount = expectedActionCount
			result.CHASMActionCount = s.GetInfo().GetActionCount()
			result.CHASMState.Paused = s.GetSchedule().GetState().GetPaused()
			if lastProcessedTime := s.Generator.Get(chasmCtx).GetLastProcessedTime(); lastProcessedTime != nil {
				result.CHASMState.LastProcessedTime = timePointer(lastProcessedTime.AsTime())
			}
			for _, start := range s.Invoker.Get(chasmCtx).GetBufferedStarts() {
				result.CHASMState.BufferedStarts = append(result.CHASMState.BufferedStarts, fmt.Sprintf(
					"workflow=%s run=%s completed=%t",
					start.GetWorkflowId(),
					start.GetRunId(),
					start.GetCompleted() != nil,
				))
			}
			if expectedActionCount != s.GetInfo().GetActionCount() {
				divergences = append(divergences, replayDivergence{
					Classification: replayClassificationSignificant,
					Kind:           "action_count",
					Message: fmt.Sprintf(
						"V1 action count is %d and CHASM action count is %d",
						expectedActionCount,
						s.GetInfo().GetActionCount(),
					),
				})
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	result.Divergences = deduplicateReplayDivergences(divergences)
	result.Classification = classifyReplayDivergences(result.Divergences)
	return result
}

func isV1ExternalInput(trace *v1HistoryTrace, event *historypb.HistoryEvent) bool {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		return true
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		_, ok := trace.watches[event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()]
		return ok
	case enumspb.EVENT_TYPE_MARKER_RECORDED:
		_, ok := trace.localWatches[event.GetEventId()]
		return ok
	default:
		return false
	}
}

func advanceCHASMTime(
	t *testing.T,
	engine *chasmtest.Engine,
	rootRef chasm.ComponentRef,
	timeSource *clock.EventTimeSource,
	currentTime *time.Time,
	target time.Time,
	inclusive bool,
	afterDrain func(time.Time),
) {
	t.Helper()
	for {
		next, ok, err := engine.NextTaskTime(rootRef, *currentTime)
		require.NoError(t, err)
		if !ok || next.After(target) || (!inclusive && next.Equal(target)) {
			break
		}
		timeSource.Update(next)
		*currentTime = next
		drainCHASMTasks(t, engine, rootRef, next)
		afterDrain(next)
	}
	timeSource.Update(target)
	*currentTime = target
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

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func deduplicateReplayDivergences(divergences []replayDivergence) []replayDivergence {
	seen := make(map[string]struct{}, len(divergences))
	result := make([]replayDivergence, 0, len(divergences))
	for _, divergence := range divergences {
		key := fmt.Sprintf("%s\x00%s\x00%s", divergence.Classification, divergence.Kind, divergence.WorkflowID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, divergence)
	}
	return result
}

func classifyReplayDivergences(divergences []replayDivergence) replayClassification {
	classification := replayClassificationMatch
	for _, divergence := range divergences {
		if replayClassificationSeverity(divergence.Classification) > replayClassificationSeverity(classification) {
			classification = divergence.Classification
		}
	}
	return classification
}

func replayClassificationSeverity(classification replayClassification) int {
	switch classification {
	case replayClassificationUnsupported:
		return 4
	case replayClassificationSignificant:
		return 3
	case replayClassificationInconclusive:
		return 2
	case replayClassificationTimingOnly:
		return 1
	default:
		return 0
	}
}

func replayResultFails(result replayCaseResult, failOn string) bool {
	switch failOn {
	case "", "significant":
		return result.Classification == replayClassificationSignificant ||
			result.Classification == replayClassificationUnsupported
	case "all":
		return result.Classification != replayClassificationMatch
	case "none":
		return false
	default:
		return true
	}
}

func writeReplayReport(path string, results []replayCaseResult) error {
	report := replayReport{
		Version: 1,
		Summary: make(map[string]int),
		Cases:   results,
	}
	for _, result := range results {
		report.Summary[string(result.Classification)]++
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
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
	handler interface {
		UpdateSchedule(context.Context, *schedulerpb.UpdateScheduleRequest) (*schedulerpb.UpdateScheduleResponse, error)
		PatchSchedule(context.Context, *schedulerpb.PatchScheduleRequest) (*schedulerpb.PatchScheduleResponse, error)
	},
	trace *v1HistoryTrace,
	event *historypb.HistoryEvent,
) error {
	attributes := event.GetWorkflowExecutionSignaledEventAttributes()
	switch attributes.GetSignalName() {
	case legacyscheduler.SignalNameUpdate:
		var update schedulespb.FullUpdateRequest
		if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &update); err != nil {
			return fmt.Errorf("decode V1 update signal: %w", err)
		}
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
		return err
	case legacyscheduler.SignalNamePatch:
		var patch schedulepb.SchedulePatch
		if err := converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &patch); err != nil {
			return fmt.Errorf("decode V1 patch signal: %w", err)
		}
		applyExpectedPatch(trace.expectedSpec, &patch)
		_, err := handler.PatchSchedule(ctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: trace.args.GetState().GetNamespaceId(),
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace: trace.args.GetState().GetNamespace(), ScheduleId: trace.args.GetState().GetScheduleId(), Patch: &patch,
			},
		})
		return err
	case legacyscheduler.SignalNameRefresh, legacyscheduler.SignalNameForceCAN:
		return nil
	case legacyscheduler.SignalNameMigrateToChasm:
		return errors.New("history contains a live migration signal")
	default:
		return fmt.Errorf("unsupported V1 scheduler signal %q", attributes.GetSignalName())
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

func observedActivityCompletion(
	t *testing.T,
	trace *v1HistoryTrace,
	event *historypb.HistoryEvent,
) (observedWatchCompletion, bool) {
	t.Helper()
	attributes := event.GetActivityTaskCompletedEventAttributes()
	watch, ok := trace.watches[attributes.GetScheduledEventId()]
	if !ok {
		return observedWatchCompletion{}, false
	}
	var response schedulespb.WatchWorkflowResponse
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &response))
	return observedWatchCompletion{request: watch.request, response: &response}, true
}

func applyObservedWatchCompletion(
	ctx context.Context,
	rootRef chasm.ComponentRef,
	workflowIDMap map[string]string,
	strictWorkflowMapping bool,
	request *schedulespb.WatchWorkflowRequest,
	response *schedulespb.WatchWorkflowResponse,
) (bool, error) {
	if response.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return true, nil
	}
	observedWorkflowID := request.GetExecution().GetWorkflowId()
	workflowID, err := resolveObservedWorkflowID(workflowIDMap, strictWorkflowMapping, observedWorkflowID)
	if err != nil {
		return false, nil
	}
	_, _, err = chasm.UpdateComponent(ctx, rootRef,
		func(s *scheduler.Scheduler, mutableCtx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker := s.Invoker.Get(mutableCtx)
			var requestID string
			bufferedState := make([]string, 0, len(invoker.GetBufferedStarts()))
			for _, start := range invoker.GetBufferedStarts() {
				bufferedState = append(bufferedState, fmt.Sprintf(
					"%s(run=%s,completed=%t)", start.GetWorkflowId(), start.GetRunId(), start.GetCompleted() != nil,
				))
				if start.GetWorkflowId() == workflowID {
					if start.GetCompleted() != nil {
						return struct{}{}, nil
					}
					requestID = start.GetRequestId()
					break
				}
			}
			if requestID == "" {
				return struct{}{}, fmt.Errorf(
					"running workflow %q (V1 workflow %q) not found in CHASM; buffered starts: %v",
					workflowID,
					observedWorkflowID,
					bufferedState,
				)
			}
			return struct{}{}, s.HandleNexusCompletion(mutableCtx, nexusCompletion(response, requestID))
		}, struct{}{})
	return err == nil, err
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
				start := observedStart{runID: response.GetRunId(), time: event.GetEventTime().AsTime()}
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
	if !historySupportsLocalActivityCapture(t, history) {
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

func historySupportsLocalActivityCapture(t *testing.T, history *historypb.History) bool {
	t.Helper()
	hasLocalActivity := false
	hasWorkflowTask := false
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			hasLocalActivity = hasLocalActivity || localActivityMetadata(t, event).ActivityType != ""
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			hasWorkflowTask = true
		default:
		}
	}
	return hasLocalActivity && hasWorkflowTask
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
