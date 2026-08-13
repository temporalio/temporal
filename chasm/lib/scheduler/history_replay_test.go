package scheduler_test

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
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
	"go.temporal.io/server/common/searchattribute/sadefs"
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
	replayReportRedactEnv         = "SCHEDULE_V2_REPLAY_REDACT"
	replayMaxDeadlinesEnv         = "SCHEDULE_V2_REPLAY_MAX_DEADLINES"
	replayMaxTasksEnv             = "SCHEDULE_V2_REPLAY_MAX_TASKS"
	replayMaxStartsEnv            = "SCHEDULE_V2_REPLAY_MAX_STARTS"
	defaultReplayMaxDeadlines     = 10_000
	defaultReplayMaxTasks         = 100_000
	defaultReplayMaxStarts        = 10_000
)

type replayClassification string

const (
	replayClassificationMatch        replayClassification = "match"
	replayClassificationTimingOnly   replayClassification = "timing_only"
	replayClassificationKnownCompat  replayClassification = "known_compatibility"
	replayClassificationSignificant  replayClassification = "significant"
	replayClassificationUnsupported  replayClassification = "unsupported"
	replayClassificationInconclusive replayClassification = "inconclusive"
)

type replayDivergence struct {
	Classification   replayClassification    `json:"classification"`
	Kind             string                  `json:"kind"`
	Message          string                  `json:"message"`
	WorkflowID       string                  `json:"workflowId,omitempty"`
	V1Time           *time.Time              `json:"v1Time,omitempty"`
	CHASMTime        *time.Time              `json:"chasmTime,omitempty"`
	Fields           []string                `json:"fields,omitempty"`
	FieldDifferences []replayFieldDifference `json:"fieldDifferences,omitempty"`
	KnownDifference  string                  `json:"knownDifference,omitempty"`
}

type replayFieldDifference struct {
	Field       string             `json:"field"`
	V1          replayValueSummary `json:"v1"`
	CHASM       replayValueSummary `json:"chasm"`
	SafeDetails []string           `json:"safeDetails,omitempty"`
}

type replayValueSummary struct {
	Present   bool     `json:"present"`
	Count     int      `json:"count,omitempty"`
	Encodings []string `json:"encodings,omitempty"`
	Digest    string   `json:"digest,omitempty"`
}

type replayCaseResult struct {
	History          string               `json:"history,omitempty"`
	Namespace        string               `json:"namespace"`
	ScheduleID       string               `json:"scheduleId"`
	Classification   replayClassification `json:"classification"`
	Cohorts          []string             `json:"cohorts,omitempty"`
	V1Starts         []string             `json:"v1Starts"`
	CHASMStarts      []string             `json:"chasmStarts"`
	V1ActionCount    int64                `json:"v1ActionCount"`
	CHASMActionCount int64                `json:"chasmActionCount"`
	CHASMState       replayStateSnapshot  `json:"chasmState"`
	ReplayStats      replayStats          `json:"replayStats"`
	Divergences      []replayDivergence   `json:"divergences,omitempty"`
}

type replayStats struct {
	Deadlines       int    `json:"deadlines"`
	PureTasks       int    `json:"pureTasks"`
	SideEffectTasks int    `json:"sideEffectTasks"`
	Starts          int    `json:"starts"`
	Cancels         int    `json:"cancels"`
	Terminates      int    `json:"terminates"`
	BudgetExceeded  string `json:"budgetExceeded,omitempty"`
}

type replayBudget struct {
	maxDeadlines int
	maxTasks     int
	maxStarts    int
	stats        replayStats
	exceeded     *replayBudgetExceededError
}

type replayBudgetExceededError struct {
	dimension string
	limit     int
}

func (e *replayBudgetExceededError) Error() string {
	return fmt.Sprintf("replay %s budget exceeded limit %d", e.dimension, e.limit)
}

type replayStateSnapshot struct {
	LastProcessedTime *time.Time `json:"lastProcessedTime,omitempty"`
	Paused            bool       `json:"paused"`
	MissedCatchup     int64      `json:"missedCatchupWindow"`
	OverlapSkipped    int64      `json:"overlapSkipped"`
	BufferDropped     int64      `json:"bufferDropped"`
	BufferedStarts    []string   `json:"bufferedStarts,omitempty"`
}

type replayReport struct {
	Version       int                       `json:"version"`
	Redacted      bool                      `json:"redacted"`
	Summary       map[string]int            `json:"summary"`
	CohortSummary map[string]map[string]int `json:"cohortSummary,omitempty"`
	Collections   []replayCollectionSummary `json:"collections,omitempty"`
	Cases         []replayCaseResult        `json:"cases"`
}

type replayCollectionSummary struct {
	Manifest          string         `json:"manifest"`
	Namespace         string         `json:"namespace"`
	Seed              string         `json:"seed"`
	ServerVersion     string         `json:"serverVersion,omitempty"`
	CollectorVersion  string         `json:"collectorVersion,omitempty"`
	ListedPopulation  int            `json:"listedPopulation"`
	Inspected         int            `json:"inspected"`
	V1Inspected       int            `json:"v1Inspected"`
	BaseSelected      int            `json:"baseSelected"`
	SelectedSchedules int            `json:"selectedSchedules"`
	CollectionStatus  map[string]int `json:"collectionStatus"`
}

type replayCollectionManifest struct {
	Namespace        string `json:"namespace"`
	Seed             string `json:"seed"`
	ServerVersion    string `json:"serverVersion"`
	CollectorVersion string `json:"collectorVersion"`
	ListedPopulation int    `json:"listedPopulation"`
	Inspected        int    `json:"inspected"`
	V1Inspected      int    `json:"v1Inspected"`
	BaseSelected     int    `json:"baseSelected"`
	Cases            []struct {
		ScheduleID string `json:"scheduleId"`
		Status     string `json:"status"`
	} `json:"cases"`
}

type observedStart struct {
	workflowID string
	runID      string
	time       time.Time
	request    *workflowservice.StartWorkflowExecutionRequest
}

type observedStartFailure struct {
	workflowID string
	time       time.Time
	request    *workflowservice.StartWorkflowExecutionRequest
}

type observedStartAttempt struct {
	workflowID                   string
	runID                        string
	time                         time.Time
	request                      *workflowservice.StartWorkflowExecutionRequest
	workflowTaskCompletedEventID int64
	failed                       bool
}

type chasmStart struct {
	workflowID string
	time       time.Time
}

type workflowExecutionKey struct {
	workflowID string
	runID      string
}

type workflowExecutionMap map[workflowExecutionKey]workflowExecutionKey

type workflowExecutionNotStartedError struct {
	execution workflowExecutionKey
}

func (e *workflowExecutionNotStartedError) Error() string {
	return fmt.Sprintf(
		"V1 workflow %q run %q completed before CHASM emitted its corresponding start; this is a scheduling/timing divergence",
		e.execution.workflowID,
		e.execution.runID,
	)
}

type ambiguousWorkflowExecutionError struct {
	workflowID string
}

func (e *ambiguousWorkflowExecutionError) Error() string {
	return fmt.Sprintf("V1 workflow %q completion does not identify a run and matches multiple CHASM executions", e.workflowID)
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
	Attempt      int32
}

type v1HistoryTrace struct {
	args          *schedulespb.StartScheduleArgs
	history       *historypb.History
	starts        []observedStart
	failedStarts  []observedStartFailure
	startAttempts []observedStartAttempt
	watches       map[int64]scheduledWatch
	startsByEvent map[int64]*schedulespb.StartWorkflowRequest
	localWatches  map[int64]observedWatchCompletion
	startTime     time.Time
	expectedSpec  *schedulepb.Schedule
	searchAttrs   *commonpb.SearchAttributes
	memo          *commonpb.Memo
	baseActions   int64
	capturedIDs   bool
	captureIssues int
	startRetries  int
}

func groupObservedStartAttempts(attempts []observedStartAttempt) map[string][]observedStartAttempt {
	grouped := make(map[string][]observedStartAttempt)
	for _, attempt := range attempts {
		if attempt.workflowID != "" {
			grouped[attempt.workflowID] = append(grouped[attempt.workflowID], attempt)
		}
	}
	return grouped
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
		redact := os.Getenv(replayReportRedactEnv) != "false"
		collections, err := readReplayCollectionSummaries(directory)
		require.NoError(t, err)
		require.NoError(t, writeReplayReportWithCollections(reportPath, results, collections, redact))
	}
}

func TestCurrentV1ConformanceCorpus(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "v1-replay", "current-v1", "*.json.gz"))
	require.NoError(t, err)
	require.Len(t, paths, 9)
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			result := replayV1HistoryAgainstCHASM(t, readReplayHistory(t, path))
			filename := filepath.Base(path)
			if filename == "skip-running.json.gz" {
				require.Equal(t, replayClassificationKnownCompat, result.Classification)
				require.Equal(t, []string{"action_time", "extra_action", "missing_action"}, uniqueReplayDivergenceKinds(result))
				require.Equal(t, result.V1ActionCount, result.CHASMActionCount)
				return
			}
			require.Equal(t, replayClassificationTimingOnly, result.Classification, result.Divergences)
			require.Equal(t, []string{"action_time"}, uniqueReplayDivergenceKinds(result))
			require.ElementsMatch(t, result.V1Starts, result.CHASMStarts)
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

	result := replayV1HistoryAgainstCHASM(t, history)
	require.Equal(t, replayClassificationInconclusive, result.Classification)
	require.Contains(t, replayDivergenceKinds(result), "action_request_unavailable")
}

func TestV1HistoryAgainstCHASM_ReplayBudgetExceeded(t *testing.T) {
	t.Setenv(replayMaxDeadlinesEnv, "1")
	startTime := time.Unix(100, 0).UTC()
	args := &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(time.Second),
			}}},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "type"}},
			}},
			State: &schedulepb.ScheduleState{},
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
			EventId: 2, EventTime: timestamppb.New(startTime.Add(5 * time.Second)), EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
			Attributes: &historypb.HistoryEvent_TimerFiredEventAttributes{
				TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{},
			},
		},
	}}

	result := replayV1HistoryAgainstCHASM(t, history)
	require.Equal(t, replayClassificationInconclusive, result.Classification)
	require.Equal(t, []string{"replay_budget_exceeded"}, replayDivergenceKinds(result))
	require.Equal(t, "deadlines", result.ReplayStats.BudgetExceeded)
	require.Empty(t, result.V1Starts)
	require.Len(t, result.CHASMStarts, 1)
}

func TestV1HistoryAgainstCHASM_OverlapPolicyExternalEffects(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		overlapPolicy      enumspb.ScheduleOverlapPolicy
		expectedCancels    int
		expectedTerminates int
	}{
		{
			name: "cancel", overlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
			expectedCancels: 1,
		},
		{
			name: "terminate", overlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
			expectedTerminates: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			startTime := time.Unix(100, 0).UTC()
			args := &schedulespb.StartScheduleArgs{
				Schedule: &schedulepb.Schedule{
					Spec: &schedulepb.ScheduleSpec{},
					Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
						StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
							WorkflowId: "action", WorkflowType: &commonpb.WorkflowType{Name: "type"},
						},
					}},
					State: &schedulepb.ScheduleState{},
				},
				Info: &schedulepb.ScheduleInfo{},
				InitialPatch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: testCase.overlapPolicy,
				}},
				State: &schedulespb.InternalState{
					Namespace: "namespace", NamespaceId: "namespace-id", ScheduleId: "schedule-id",
					LastProcessedTime: timestamppb.New(startTime),
					BufferedStarts: []*schedulespb.BufferedStart{{
						WorkflowId: "running-workflow", RunId: "running-run-id",
					}},
				},
			}
			history := &historypb.History{Events: []*historypb.HistoryEvent{{
				EventId: 1, EventTime: timestamppb.New(startTime), EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: mustPayloads(t, args)},
				},
			}}}

			result := replayV1HistoryAgainstCHASM(t, history)
			require.Equal(t, testCase.expectedCancels, result.ReplayStats.Cancels)
			require.Equal(t, testCase.expectedTerminates, result.ReplayStats.Terminates)
		})
	}
}

func TestV1HistoryAgainstCHASM_MissingInitialState(t *testing.T) {
	history := &historypb.History{Events: []*historypb.HistoryEvent{{
		EventId: 1, EventTime: timestamppb.Now(), EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}}}

	result := replayV1HistoryAgainstCHASM(t, history)
	require.Equal(t, replayClassificationUnsupported, result.Classification)
	require.Equal(t, []string{"initial_state_unavailable"}, replayDivergenceKinds(result))
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

func TestExtractV1HistoryTraceCapturesStartFailure(t *testing.T) {
	args := &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{}, State: &schedulepb.ScheduleState{}},
		Info:     &schedulepb.ScheduleInfo{},
		State:    &schedulespb.InternalState{Namespace: "namespace", NamespaceId: "namespace-id", ScheduleId: "schedule-id"},
	}
	request := &schedulespb.StartWorkflowRequest{Request: &workflowservice.StartWorkflowExecutionRequest{WorkflowId: "failed-workflow"}}
	history := &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: mustPayloads(t, args)},
			},
		},
		{
			EventId: 2, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityType: &commonpb.ActivityType{Name: "StartWorkflow"}, Input: mustPayloads(t, request),
				},
			},
		},
		{
			EventId: 3, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
			Attributes: &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
				ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{ScheduledEventId: 2},
			},
		},
	}}
	trace := extractV1HistoryTrace(t, history)
	require.Empty(t, trace.starts)
	require.Len(t, trace.failedStarts, 1)
	require.Equal(t, "failed-workflow", trace.failedStarts[0].workflowID)
	require.Len(t, trace.startAttempts, 1)
	require.True(t, trace.startAttempts[0].failed)
}

func TestGroupObservedStartAttemptsPreservesOutcomeOrder(t *testing.T) {
	attempts := []observedStartAttempt{
		{workflowID: "same-id", runID: "successful-run"},
		{workflowID: "other-id", failed: true},
		{workflowID: "same-id", failed: true},
	}

	grouped := groupObservedStartAttempts(attempts)
	require.Equal(t, []observedStartAttempt{
		{workflowID: "same-id", runID: "successful-run"},
		{workflowID: "same-id", failed: true},
	}, grouped["same-id"])
}

func TestNormalizeScheduleForComparison(t *testing.T) {
	expected := &schedulepb.Schedule{
		Spec:  &schedulepb.ScheduleSpec{},
		State: &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 2},
	}
	actual := &schedulepb.Schedule{
		Spec:     &schedulepb.ScheduleSpec{},
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{LimitedActions: true},
	}
	protorequire.ProtoEqual(t, normalizeScheduleForComparison(expected), normalizeScheduleForComparison(actual))
}

func TestApplyExpectedPatchHandlesUnavailableState(t *testing.T) {
	applyExpectedPatch(nil, &schedulepb.SchedulePatch{Pause: "pause"})

	schedule := &schedulepb.Schedule{}
	applyExpectedPatch(schedule, &schedulepb.SchedulePatch{Pause: "pause"})
	require.True(t, schedule.GetState().GetPaused())
	require.Equal(t, "pause", schedule.GetState().GetNotes())
}

func TestResolveObservedWorkflowExecution(t *testing.T) {
	executionMap := workflowExecutionMap{
		{workflowID: "v1-workflow", runID: "v1-run"}: {workflowID: "chasm-workflow", runID: "chasm-run"},
	}

	execution, err := resolveObservedWorkflowExecution(executionMap, true, workflowExecutionKey{workflowID: "v1-workflow", runID: "v1-run"})
	require.NoError(t, err)
	require.Equal(t, workflowExecutionKey{workflowID: "chasm-workflow", runID: "chasm-run"}, execution)

	execution, err = resolveObservedWorkflowExecution(executionMap, false, workflowExecutionKey{workflowID: "unmapped-workflow", runID: "unmapped-run"})
	require.NoError(t, err)
	require.Equal(t, workflowExecutionKey{workflowID: "unmapped-workflow", runID: "unmapped-run"}, execution)

	_, err = resolveObservedWorkflowExecution(executionMap, true, workflowExecutionKey{workflowID: "not-started", runID: "not-started-run"})
	require.EqualError(t, err,
		`V1 workflow "not-started" run "not-started-run" completed before CHASM emitted its corresponding start; this is a scheduling/timing divergence`,
	)
}

func TestResolveObservedWorkflowExecutionWithoutRunID(t *testing.T) {
	executionMap := workflowExecutionMap{
		{workflowID: "v1-workflow", runID: "v1-run"}: {workflowID: "chasm-workflow", runID: "chasm-run"},
	}

	execution, err := resolveObservedWorkflowExecution(executionMap, true, workflowExecutionKey{workflowID: "v1-workflow"})
	require.NoError(t, err)
	require.Equal(t, workflowExecutionKey{workflowID: "chasm-workflow", runID: "chasm-run"}, execution)

	executionMap[workflowExecutionKey{workflowID: "v1-workflow", runID: "v1-run-2"}] = workflowExecutionKey{workflowID: "chasm-workflow", runID: "chasm-run-2"}
	_, err = resolveObservedWorkflowExecution(executionMap, true, workflowExecutionKey{workflowID: "v1-workflow"})
	require.EqualError(t, err, `V1 workflow "v1-workflow" completion does not identify a run and matches multiple CHASM executions`)
	divergence := replayCompletionErrorDivergence(err, &schedulespb.WatchWorkflowRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "v1-workflow"},
	}, time.Unix(100, 0), true)
	require.Equal(t, replayClassificationInconclusive, divergence.Classification)
	require.Equal(t, "ambiguous_completion", divergence.Kind)
}

func TestSeedCarriedWorkflowExecutions(t *testing.T) {
	executionMap := make(workflowExecutionMap)
	seedCarriedWorkflowExecutions(executionMap, []*schedulespb.BufferedStart{
		{WorkflowId: "running", RunId: "running-run"},
		{WorkflowId: "pending"},
		{WorkflowId: "completed", RunId: "completed-run", Completed: &schedulespb.CompletedResult{}},
	})

	require.Equal(t, workflowExecutionMap{
		{workflowID: "running", runID: "running-run"}: {workflowID: "running", runID: "running-run"},
	}, executionMap)
}

func TestReplayReport(t *testing.T) {
	results := []replayCaseResult{
		{Namespace: "customer", ScheduleID: "matching", Cohorts: []string{"spec_interval"}, Classification: replayClassificationMatch},
		{ScheduleID: "different", Classification: replayClassificationSignificant},
	}
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, writeReplayReport(path, results, false))
	reportInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), reportInfo.Mode().Perm())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var report replayReport
	require.NoError(t, json.Unmarshal(data, &report))
	require.Equal(t, 4, report.Version)
	require.False(t, report.Redacted)
	require.Equal(t, map[string]int{"match": 1, "significant": 1}, report.Summary)
	require.Equal(t, map[string]map[string]int{"spec_interval": {"match": 1}}, report.CohortSummary)
	require.Equal(t, results, report.Cases)

	redactedPath := filepath.Join(t.TempDir(), "redacted-report.json")
	require.NoError(t, writeReplayReport(redactedPath, results, true))
	redactedData, err := os.ReadFile(redactedPath)
	require.NoError(t, err)
	require.NotContains(t, string(redactedData), "customer")
	require.NotContains(t, string(redactedData), "matching")
	require.Contains(t, string(redactedData), "sha256:")

	require.True(t, replayResultFails(results[1], "significant"))
	require.False(t, replayResultFails(results[1], "none"))
	require.True(t, replayResultFails(replayCaseResult{Classification: replayClassificationTimingOnly}, "all"))
	require.False(t, replayResultFails(replayCaseResult{Classification: replayClassificationKnownCompat}, "significant"))
	require.True(t, replayResultFails(replayCaseResult{Classification: replayClassificationKnownCompat}, "all"))
}

func TestNormalizeStartWorkflowRequest(t *testing.T) {
	v1 := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "namespace", Identity: "v1", RequestId: "v1-request",
		WorkflowId: "workflow", Input: mustPayloads(t, "input"),
	}
	chasmRequest := proto.CloneOf(v1)
	chasmRequest.Namespace = ""
	chasmRequest.Identity = "chasm"
	chasmRequest.RequestId = "chasm-request"
	chasmRequest.LastCompletionResult = &commonpb.Payloads{}
	protorequire.ProtoEqual(t, normalizeStartWorkflowRequest(v1), normalizeStartWorkflowRequest(chasmRequest))
	chasmRequest.Input = mustPayloads(t, "different")
	require.False(t, proto.Equal(normalizeStartWorkflowRequest(v1), normalizeStartWorkflowRequest(chasmRequest)))
}

func TestStartWorkflowRequestFieldDifferences(t *testing.T) {
	v1ScheduledTime := time.Unix(100, 0).UTC()
	chasmScheduledTime := v1ScheduledTime.Add(1500 * time.Millisecond)
	v1ScheduledPayload, err := converter.GetDefaultDataConverter().ToPayload(v1ScheduledTime)
	require.NoError(t, err)
	chasmScheduledPayload, err := converter.GetDefaultDataConverter().ToPayload(chasmScheduledTime)
	require.NoError(t, err)
	v1 := &workflowservice.StartWorkflowExecutionRequest{
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			sadefs.TemporalScheduledStartTime: v1ScheduledPayload,
		}},
		LastCompletionResult: mustPayloads(t, "one", "two"),
		ContinuedFailure: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{},
		},
	}
	chasmRequest := &workflowservice.StartWorkflowExecutionRequest{
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			sadefs.TemporalScheduledStartTime: chasmScheduledPayload,
		}},
		LastCompletionResult: mustPayloads(t, "one"),
		ContinuedFailure: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{},
		},
	}
	fields := startWorkflowRequestDifferenceFields(v1, chasmRequest)
	differences := startWorkflowRequestFieldDifferences(v1, chasmRequest, fields)

	require.ElementsMatch(t, []string{"continued_failure", "last_completion_result", "search_attributes"}, fields)
	require.Len(t, differences, 3)
	var searchDetails []string
	for _, difference := range differences {
		require.NotEmpty(t, difference.V1.Digest)
		require.NotEmpty(t, difference.CHASM.Digest)
		if difference.Field == "search_attributes" {
			searchDetails = difference.SafeDetails
		}
	}
	require.NotEmpty(t, searchDetails)
	require.Contains(t, searchDetails[0], sadefs.TemporalScheduledStartTime)

	redacted := redactReplayResults([]replayCaseResult{{Divergences: []replayDivergence{{FieldDifferences: differences}}}})
	for _, difference := range redacted[0].Divergences[0].FieldDifferences {
		require.Empty(t, difference.V1.Digest)
		require.Empty(t, difference.CHASM.Digest)
	}
}

func TestNormalizeStartWorkflowRequestIgnoresScheduledTimeSubsecondPrecision(t *testing.T) {
	v1ScheduledPayload, err := sadefs.EncodeValue(time.Unix(100, 0).UTC(), enumspb.INDEXED_VALUE_TYPE_DATETIME)
	require.NoError(t, err)
	chasmScheduledPayload, err := sadefs.EncodeValue(time.Unix(100, 500_000_000).UTC(), enumspb.INDEXED_VALUE_TYPE_DATETIME)
	require.NoError(t, err)
	v1 := &workflowservice.StartWorkflowExecutionRequest{SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		sadefs.TemporalScheduledStartTime: v1ScheduledPayload,
	}}}
	chasmRequest := &workflowservice.StartWorkflowExecutionRequest{SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
		sadefs.TemporalScheduledStartTime: chasmScheduledPayload,
	}}}

	protorequire.ProtoEqual(t, normalizeStartWorkflowRequest(v1), normalizeStartWorkflowRequest(chasmRequest))
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
		Cohorts:        replayHistoryCohorts(trace),
	}
	budget := newReplayBudget(t)
	for _, start := range trace.starts {
		result.V1Starts = append(result.V1Starts, start.workflowID)
	}
	if trace.captureIssues != 0 {
		result.Classification = replayClassificationInconclusive
		result.Divergences = []replayDivergence{{
			Classification: replayClassificationInconclusive,
			Kind:           "local_activity_capture_unavailable",
			Message:        fmt.Sprintf("%d V1 local activity marker(s) could not be associated with replayed invocations", trace.captureIssues),
		}}
		return result
	}
	if trace.args.GetSchedule() == nil || trace.args.GetState() == nil {
		result.Classification = replayClassificationUnsupported
		result.Divergences = []replayDivergence{{
			Classification: replayClassificationUnsupported,
			Kind:           "initial_state_unavailable",
			Message:        "V1 history does not contain the schedule and internal state required to initialize CHASM replay",
		}}
		return result
	}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	ctrl := gomock.NewController(t)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)

	var actionMu sync.Mutex
	startIndex := 0
	executionMap := make(workflowExecutionMap)
	var chasmStarts []chasmStart
	var divergences []replayDivergence
	if trace.startRetries != 0 {
		divergences = append(divergences, replayDivergence{
			Classification: replayClassificationInconclusive,
			Kind:           "start_retry_unobservable",
			Message:        fmt.Sprintf("%d V1 StartWorkflow local activities retried without recording the intermediate failure", trace.startRetries),
		})
	}
	startsByWorkflowID := make(map[string][]observedStart, len(trace.starts))
	for _, start := range trace.starts {
		if start.workflowID != "" {
			startsByWorkflowID[start.workflowID] = append(startsByWorkflowID[start.workflowID], start)
		}
	}
	attemptsByWorkflowID := groupObservedStartAttempts(trace.startAttempts)
	consumedAttempts := make(map[string]int, len(attemptsByWorkflowID))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(trace.startTime)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			actionMu.Lock()
			defer actionMu.Unlock()
			budget.addStart()
			workflowID := request.GetWorkflowId()
			chasmTime := timeSource.Now().UTC()
			if attempts := attemptsByWorkflowID[workflowID]; consumedAttempts[workflowID] < len(attempts) {
				attempt := attempts[consumedAttempts[workflowID]]
				consumedAttempts[workflowID]++
				if !proto.Equal(normalizeStartWorkflowRequest(attempt.request), normalizeStartWorkflowRequest(request)) {
					fields := startWorkflowRequestDifferenceFields(attempt.request, request)
					message := "V1 and CHASM emitted different StartWorkflowExecution requests"
					if attempt.failed {
						message = "V1 and CHASM emitted different failed StartWorkflowExecution requests"
					}
					classification := replayClassificationSignificant
					knownDifference := ""
					if slices.Contains(fields, "input") &&
						workflowTaskContainsUpdateBeforeStart(trace.history, attempt.workflowTaskCompletedEventID) {
						classification = replayClassificationInconclusive
						knownDifference = "v1_workflow_task_input_ordering"
						message += "; V1 processed an update delivered with the same workflow task, after the replay had already fired the nominal CHASM deadline"
					}
					divergences = append(divergences, replayDivergence{
						Classification:   classification,
						Kind:             "action_request",
						Message:          message,
						WorkflowID:       workflowID,
						V1Time:           timePointer(attempt.time),
						CHASMTime:        timePointer(chasmTime),
						Fields:           fields,
						FieldDifferences: startWorkflowRequestFieldDifferences(attempt.request, request, fields),
						KnownDifference:  knownDifference,
					})
				}
				if attempt.failed {
					return nil, serviceerror.NewInvalidArgument("replayed V1 StartWorkflow failure")
				}
				chasmStarts = append(chasmStarts, chasmStart{workflowID: workflowID, time: chasmTime})
				executionMap[workflowExecutionKey{workflowID: attempt.workflowID, runID: attempt.runID}] = workflowExecutionKey{
					workflowID: request.GetWorkflowId(),
					runID:      attempt.runID,
				}
				startIndex++
				if !attempt.time.IsZero() && !attempt.time.Equal(chasmTime) {
					divergences = append(divergences, replayDivergence{
						Classification: replayClassificationTimingOnly,
						Kind:           "action_time",
						Message:        "V1 and CHASM emitted the same workflow start at different observed times",
						WorkflowID:     workflowID,
						V1Time:         timePointer(attempt.time),
						CHASMTime:      timePointer(chasmTime),
					})
				}
				return &workflowservice.StartWorkflowExecutionResponse{RunId: attempt.runID}, nil
			}
			chasmStarts = append(chasmStarts, chasmStart{workflowID: workflowID, time: chasmTime})
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
	historyClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, _ *historyservice.RequestCancelWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
			actionMu.Lock()
			defer actionMu.Unlock()
			budget.stats.Cancels++
			return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
		})
	historyClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, _ *historyservice.TerminateWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
			actionMu.Lock()
			defer actionMu.Unlock()
			budget.stats.Terminates++
			return &historyservice.TerminateWorkflowExecutionResponse{}, nil
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
	seedCarriedWorkflowExecutions(executionMap, migrationRequest.GetState().GetInvokerState().GetBufferedStarts())
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
	if err := drainCHASMTasks(engine, rootRef, trace.startTime, budget); err != nil {
		return replayBudgetExceededResult(t, result, budget, chasmStarts, err)
	}
	currentTime := trace.startTime
	var pendingCompletions []observedWatchCompletion
	applyPendingCompletions := func(now time.Time) error {
		for {
			appliedAny := false
			remaining := pendingCompletions[:0]
			for _, completion := range pendingCompletions {
				applied, err := applyObservedWatchCompletion(
					engineCtx,
					rootRef,
					executionMap,
					trace.capturedIDs,
					completion.request,
					completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayCompletionErrorDivergence(err, completion.request, now, false))
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
				return nil
			}
			if err := drainCHASMTasks(engine, rootRef, now, budget); err != nil {
				return err
			}
		}
	}

	for _, event := range trace.history.GetEvents()[1:] {
		now := event.GetEventTime().AsTime()
		if now.Before(currentTime) {
			now = currentTime
		}
		if err := advanceCHASMTime(
			engine,
			rootRef,
			timeSource,
			budget,
			&currentTime,
			now,
			!isV1ExternalInput(trace, event),
			applyPendingCompletions,
		); err != nil {
			return replayBudgetExceededResult(t, result, budget, chasmStarts, err)
		}
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
					engineCtx, rootRef, executionMap, trace.capturedIDs, completion.request, completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayCompletionErrorDivergence(err, completion.request, now, true))
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
					engineCtx, rootRef, executionMap, trace.capturedIDs, completion.request, completion.response,
				)
				if err != nil {
					divergences = append(divergences, replayCompletionErrorDivergence(err, completion.request, now, true))
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
		if err := drainCHASMTasks(engine, rootRef, now, budget); err != nil {
			return replayBudgetExceededResult(t, result, budget, chasmStarts, err)
		}
		if err := applyPendingCompletions(now); err != nil {
			return replayBudgetExceededResult(t, result, budget, chasmStarts, err)
		}
	}

	actionMu.Lock()
	checkedAttempts := make(map[string]int, len(trace.startAttempts))
	for _, attempt := range trace.startAttempts {
		if attempt.workflowID != "" {
			position := checkedAttempts[attempt.workflowID]
			checkedAttempts[attempt.workflowID]++
			if consumedAttempts[attempt.workflowID] <= position {
				kind := "missing_action"
				message := fmt.Sprintf("V1 emitted workflow start %q but CHASM did not", attempt.workflowID)
				if attempt.failed {
					kind = "missing_action_attempt"
					message = fmt.Sprintf("V1 attempted workflow start %q but CHASM did not", attempt.workflowID)
				}
				divergences = append(divergences, replayDivergence{
					Classification: replayClassificationSignificant,
					Kind:           kind,
					Message:        message,
					WorkflowID:     attempt.workflowID,
					V1Time:         timePointer(attempt.time),
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
	if len(trace.starts) != 0 && len(startsByWorkflowID) == 0 {
		divergences = append(divergences, replayDivergence{
			Classification: replayClassificationInconclusive,
			Kind:           "action_request_unavailable",
			Message:        "V1 history did not expose StartWorkflowExecution requests for field-level comparison",
		})
	}
	for _, start := range chasmStarts {
		result.CHASMStarts = append(result.CHASMStarts, start.workflowID)
	}
	actionMu.Unlock()
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
			result.CHASMState.MissedCatchup = s.GetInfo().GetMissedCatchupWindow()
			result.CHASMState.OverlapSkipped = s.GetInfo().GetOverlapSkipped()
			result.CHASMState.BufferDropped = s.GetInfo().GetBufferDropped()
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
	annotateKnownCompatibilityDivergences(result.Divergences, result.Cohorts)
	result.Classification = classifyReplayDivergences(result.Divergences)
	result.ReplayStats = budget.stats
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
	engine *chasmtest.Engine,
	rootRef chasm.ComponentRef,
	timeSource *clock.EventTimeSource,
	budget *replayBudget,
	currentTime *time.Time,
	target time.Time,
	inclusive bool,
	afterDrain func(time.Time) error,
) error {
	for {
		next, ok, err := engine.NextTaskTime(rootRef, *currentTime)
		if err != nil {
			return err
		}
		if !ok || next.After(target) || (!inclusive && next.Equal(target)) {
			break
		}
		if err := budget.addDeadline(); err != nil {
			return err
		}
		timeSource.Update(next)
		*currentTime = next
		if err := drainCHASMTasks(engine, rootRef, next, budget); err != nil {
			return err
		}
		if err := afterDrain(next); err != nil {
			return err
		}
	}
	timeSource.Update(target)
	*currentTime = target
	return nil
}

func normalizeScheduleForComparison(schedule *schedulepb.Schedule) *schedulepb.Schedule {
	normalized := proto.CloneOf(schedule)
	if normalized.GetState() != nil {
		// Workflow history exposes action decisions but not enough information to
		// reconstruct whether every observed start consumed the scheduled-action
		// limit (manual triggers and backfills do not). Compare durable
		// configuration here and report action counts separately.
		normalized.State.LimitedActions = false
		normalized.State.RemainingActions = 0
	}
	if proto.Equal(normalized.GetPolicies(), &schedulepb.SchedulePolicies{}) {
		normalized.Policies = nil
	}
	if proto.Equal(normalized.GetState(), &schedulepb.ScheduleState{}) {
		normalized.State = nil
	}
	return normalized
}

func normalizeStartWorkflowRequest(request *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionRequest {
	if request == nil {
		return nil
	}
	normalized := proto.CloneOf(request)
	normalized.Identity = ""
	normalized.Namespace = ""
	normalized.RequestId = ""
	normalized.CompletionCallbacks = nil
	normalized.RequestEagerExecution = false
	if len(normalized.GetLastCompletionResult().GetPayloads()) == 0 {
		normalized.LastCompletionResult = nil
	}
	for name, payload := range normalized.GetSearchAttributes().GetIndexedFields() {
		if name == sadefs.TemporalScheduledStartTime {
			if scheduledTime, ok := decodeDateTimeSearchAttribute(payload); ok {
				encoded, err := sadefs.EncodeValue(scheduledTime.Truncate(time.Second), enumspb.INDEXED_VALUE_TYPE_DATETIME)
				if err == nil {
					normalized.SearchAttributes.IndexedFields[name] = encoded
					payload = encoded
				}
			}
		}
		if strings.HasPrefix(name, "TemporalScheduled") {
			delete(payload.Metadata, "type")
		}
	}
	return normalized
}

func startWorkflowRequestDifferenceFields(
	v1 *workflowservice.StartWorkflowExecutionRequest,
	chasmRequest *workflowservice.StartWorkflowExecutionRequest,
) []string {
	v1 = normalizeStartWorkflowRequest(v1)
	chasmRequest = normalizeStartWorkflowRequest(chasmRequest)
	if v1 == nil || chasmRequest == nil {
		return []string{"request"}
	}
	var fields []string
	addScalar := func(name string, different bool) {
		if different {
			fields = append(fields, name)
		}
	}
	addProto := func(name string, v1Value, chasmValue proto.Message) {
		if !proto.Equal(v1Value, chasmValue) {
			fields = append(fields, name)
		}
	}
	addScalar("workflow_id", v1.GetWorkflowId() != chasmRequest.GetWorkflowId())
	addProto("workflow_type", v1.GetWorkflowType(), chasmRequest.GetWorkflowType())
	addProto("task_queue", v1.GetTaskQueue(), chasmRequest.GetTaskQueue())
	addProto("input", v1.GetInput(), chasmRequest.GetInput())
	addProto("workflow_execution_timeout", v1.GetWorkflowExecutionTimeout(), chasmRequest.GetWorkflowExecutionTimeout())
	addProto("workflow_run_timeout", v1.GetWorkflowRunTimeout(), chasmRequest.GetWorkflowRunTimeout())
	addProto("workflow_task_timeout", v1.GetWorkflowTaskTimeout(), chasmRequest.GetWorkflowTaskTimeout())
	addScalar("workflow_id_reuse_policy", v1.GetWorkflowIdReusePolicy() != chasmRequest.GetWorkflowIdReusePolicy())
	addScalar("workflow_id_conflict_policy", v1.GetWorkflowIdConflictPolicy() != chasmRequest.GetWorkflowIdConflictPolicy())
	addProto("retry_policy", v1.GetRetryPolicy(), chasmRequest.GetRetryPolicy())
	addScalar("cron_schedule", v1.GetCronSchedule() != chasmRequest.GetCronSchedule())
	addProto("memo", v1.GetMemo(), chasmRequest.GetMemo())
	addProto("search_attributes", v1.GetSearchAttributes(), chasmRequest.GetSearchAttributes())
	addProto("header", v1.GetHeader(), chasmRequest.GetHeader())
	addProto("continued_failure", v1.GetContinuedFailure(), chasmRequest.GetContinuedFailure())
	addProto("last_completion_result", v1.GetLastCompletionResult(), chasmRequest.GetLastCompletionResult())
	addProto("workflow_start_delay", v1.GetWorkflowStartDelay(), chasmRequest.GetWorkflowStartDelay())
	addProto("user_metadata", v1.GetUserMetadata(), chasmRequest.GetUserMetadata())
	addProto("versioning_override", v1.GetVersioningOverride(), chasmRequest.GetVersioningOverride())
	addProto("on_conflict_options", v1.GetOnConflictOptions(), chasmRequest.GetOnConflictOptions())
	addProto("priority", v1.GetPriority(), chasmRequest.GetPriority())
	addProto("eager_worker_deployment_options", v1.GetEagerWorkerDeploymentOptions(), chasmRequest.GetEagerWorkerDeploymentOptions())
	addProto("time_skipping_config", v1.GetTimeSkippingConfig(), chasmRequest.GetTimeSkippingConfig())
	linksDiffer := len(v1.GetLinks()) != len(chasmRequest.GetLinks())
	if !linksDiffer {
		for index := range v1.GetLinks() {
			if !proto.Equal(v1.GetLinks()[index], chasmRequest.GetLinks()[index]) {
				linksDiffer = true
				break
			}
		}
	}
	addScalar("links", linksDiffer)
	if len(fields) == 0 && !proto.Equal(v1, chasmRequest) {
		fields = append(fields, "other")
	}
	return fields
}

func startWorkflowRequestFieldDifferences(
	v1 *workflowservice.StartWorkflowExecutionRequest,
	chasmRequest *workflowservice.StartWorkflowExecutionRequest,
	fields []string,
) []replayFieldDifference {
	v1 = normalizeStartWorkflowRequest(v1)
	chasmRequest = normalizeStartWorkflowRequest(chasmRequest)
	differences := make([]replayFieldDifference, 0, len(fields))
	for _, field := range fields {
		difference := replayFieldDifference{
			Field: field,
			V1:    summarizeReplayValue(startWorkflowRequestFieldMessage(v1, field)),
			CHASM: summarizeReplayValue(startWorkflowRequestFieldMessage(chasmRequest, field)),
		}
		switch field {
		case "search_attributes":
			difference.SafeDetails = searchAttributeSafeDifferences(v1.GetSearchAttributes(), chasmRequest.GetSearchAttributes())
		case "continued_failure":
			difference.SafeDetails = []string{
				fmt.Sprintf("v1_failure_info=%T", v1.GetContinuedFailure().GetFailureInfo()),
				fmt.Sprintf("chasm_failure_info=%T", chasmRequest.GetContinuedFailure().GetFailureInfo()),
			}
		default:
		}
		differences = append(differences, difference)
	}
	return differences
}

func startWorkflowRequestFieldMessage(
	request *workflowservice.StartWorkflowExecutionRequest,
	field string,
) proto.Message {
	if request == nil {
		return nil
	}
	switch field {
	case "workflow_type":
		return request.GetWorkflowType()
	case "task_queue":
		return request.GetTaskQueue()
	case "input":
		return request.GetInput()
	case "workflow_execution_timeout":
		return request.GetWorkflowExecutionTimeout()
	case "workflow_run_timeout":
		return request.GetWorkflowRunTimeout()
	case "workflow_task_timeout":
		return request.GetWorkflowTaskTimeout()
	case "retry_policy":
		return request.GetRetryPolicy()
	case "memo":
		return request.GetMemo()
	case "search_attributes":
		return request.GetSearchAttributes()
	case "header":
		return request.GetHeader()
	case "continued_failure":
		return request.GetContinuedFailure()
	case "last_completion_result":
		return request.GetLastCompletionResult()
	case "workflow_start_delay":
		return request.GetWorkflowStartDelay()
	case "user_metadata":
		return request.GetUserMetadata()
	case "versioning_override":
		return request.GetVersioningOverride()
	case "on_conflict_options":
		return request.GetOnConflictOptions()
	case "priority":
		return request.GetPriority()
	case "eager_worker_deployment_options":
		return request.GetEagerWorkerDeploymentOptions()
	case "time_skipping_config":
		return request.GetTimeSkippingConfig()
	default:
		return nil
	}
}

func summarizeReplayValue(message proto.Message) replayValueSummary {
	if message == nil || !message.ProtoReflect().IsValid() {
		return replayValueSummary{}
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return replayValueSummary{Present: true}
	}
	digest := sha256.Sum256(data)
	summary := replayValueSummary{Present: true, Count: 1, Digest: fmt.Sprintf("sha256:%x", digest[:12])}
	var payloads []*commonpb.Payload
	switch value := message.(type) {
	case *commonpb.Payloads:
		payloads = value.GetPayloads()
		summary.Count = len(payloads)
	case *commonpb.SearchAttributes:
		summary.Count = len(value.GetIndexedFields())
		for _, payload := range value.GetIndexedFields() {
			payloads = append(payloads, payload)
		}
	case *commonpb.Memo:
		summary.Count = len(value.GetFields())
		for _, payload := range value.GetFields() {
			payloads = append(payloads, payload)
		}
	case *commonpb.Header:
		summary.Count = len(value.GetFields())
		for _, payload := range value.GetFields() {
			payloads = append(payloads, payload)
		}
	default:
	}
	encodings := make(map[string]struct{})
	for _, payload := range payloads {
		if encoding := string(payload.GetMetadata()["encoding"]); encoding != "" {
			encodings[encoding] = struct{}{}
		}
	}
	for encoding := range encodings {
		summary.Encodings = append(summary.Encodings, encoding)
	}
	sort.Strings(summary.Encodings)
	return summary
}

func searchAttributeSafeDifferences(v1, chasmValue *commonpb.SearchAttributes) []string {
	keys := make(map[string]struct{})
	for key := range v1.GetIndexedFields() {
		keys[key] = struct{}{}
	}
	for key := range chasmValue.GetIndexedFields() {
		keys[key] = struct{}{}
	}
	orderedKeys := make([]string, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)
	result := make([]string, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		v1Payload, v1Present := v1.GetIndexedFields()[key]
		chasmPayload, chasmPresent := chasmValue.GetIndexedFields()[key]
		if proto.Equal(v1Payload, chasmPayload) {
			continue
		}
		keyReference := replayOpaqueID(key)
		if sadefs.IsReserved(key) {
			keyReference = key
		}
		if key == sadefs.TemporalScheduledStartTime {
			v1Time, v1OK := decodeDateTimeSearchAttribute(v1Payload)
			chasmTime, chasmOK := decodeDateTimeSearchAttribute(chasmPayload)
			if v1OK && chasmOK {
				result = append(result, fmt.Sprintf("%s:v1=%s,chasm=%s", key, v1Time.Format(time.RFC3339Nano), chasmTime.Format(time.RFC3339Nano)))
				continue
			}
		}
		result = append(result, fmt.Sprintf("%s:v1_present=%t,chasm_present=%t", keyReference, v1Present, chasmPresent))
	}
	return result
}

func decodeDateTimeSearchAttribute(payload *commonpb.Payload) (time.Time, bool) {
	value, err := sadefs.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_DATETIME, false)
	if err != nil {
		return time.Time{}, false
	}
	decoded, ok := value.(time.Time)
	return decoded, ok
}

func replayHistoryCohorts(trace *v1HistoryTrace) []string {
	cohorts := make(map[string]struct{})
	if len(trace.failedStarts) != 0 {
		cohorts["start_failure"] = struct{}{}
	}
	spec := trace.args.GetSchedule().GetSpec()
	if len(spec.GetInterval()) != 0 {
		cohorts["spec_interval"] = struct{}{}
	}
	if len(spec.GetCalendar()) != 0 || len(spec.GetStructuredCalendar()) != 0 {
		cohorts["spec_calendar"] = struct{}{}
	}
	if len(spec.GetCronString()) != 0 {
		cohorts["spec_cron"] = struct{}{}
	}
	if trace.args.GetSchedule().GetState().GetPaused() {
		cohorts["paused"] = struct{}{}
	}
	overlap := trace.args.GetSchedule().GetPolicies().GetOverlapPolicy().String()
	cohorts["overlap_"+strings.ToLower(strings.TrimPrefix(overlap, "SCHEDULE_OVERLAP_POLICY_"))] = struct{}{}
	for _, event := range trace.history.GetEvents() {
		if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			continue
		}
		attributes := event.GetWorkflowExecutionSignaledEventAttributes()
		switch attributes.GetSignalName() {
		case legacyscheduler.SignalNameUpdate:
			cohorts["update"] = struct{}{}
		case legacyscheduler.SignalNamePatch:
			var patch schedulepb.SchedulePatch
			if converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &patch) == nil {
				if len(patch.GetBackfillRequest()) != 0 {
					cohorts["backfill"] = struct{}{}
				}
				if patch.GetPause() != "" || patch.GetUnpause() != "" {
					cohorts["pause_interaction"] = struct{}{}
				}
			}
		}
	}
	result := make([]string, 0, len(cohorts))
	for cohort := range cohorts {
		result = append(result, cohort)
	}
	sort.Strings(result)
	return result
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func newReplayBudget(t *testing.T) *replayBudget {
	t.Helper()
	return &replayBudget{
		maxDeadlines: replayLimitFromEnvironment(t, replayMaxDeadlinesEnv, defaultReplayMaxDeadlines),
		maxTasks:     replayLimitFromEnvironment(t, replayMaxTasksEnv, defaultReplayMaxTasks),
		maxStarts:    replayLimitFromEnvironment(t, replayMaxStartsEnv, defaultReplayMaxStarts),
	}
}

func replayLimitFromEnvironment(t *testing.T, name string, defaultValue int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	limit, err := strconv.Atoi(value)
	require.NoError(t, err, "%s must be an integer", name)
	require.Positive(t, limit, "%s must be positive", name)
	return limit
}

func (b *replayBudget) addDeadline() error {
	b.stats.Deadlines++
	return b.check("deadlines", b.stats.Deadlines, b.maxDeadlines)
}

func (b *replayBudget) addTasks(pure, sideEffect int) error {
	b.stats.PureTasks += pure
	b.stats.SideEffectTasks += sideEffect
	return b.check("tasks", b.stats.PureTasks+b.stats.SideEffectTasks, b.maxTasks)
}

func (b *replayBudget) addStart() {
	b.stats.Starts++
	_ = b.check("starts", b.stats.Starts, b.maxStarts)
}

func (b *replayBudget) check(dimension string, value, limit int) error {
	if b.exceeded != nil {
		return b.exceeded
	}
	if value <= limit {
		return nil
	}
	b.exceeded = &replayBudgetExceededError{dimension: dimension, limit: limit}
	b.stats.BudgetExceeded = dimension
	return b.exceeded
}

func replayBudgetExceededResult(
	t *testing.T,
	result replayCaseResult,
	budget *replayBudget,
	chasmStarts []chasmStart,
	err error,
) replayCaseResult {
	t.Helper()
	var budgetErr *replayBudgetExceededError
	require.ErrorAs(t, err, &budgetErr)
	result.ReplayStats = budget.stats
	for _, start := range chasmStarts {
		result.CHASMStarts = append(result.CHASMStarts, start.workflowID)
	}
	result.Classification = replayClassificationInconclusive
	result.Divergences = []replayDivergence{{
		Classification: replayClassificationInconclusive,
		Kind:           "replay_budget_exceeded",
		Message:        budgetErr.Error(),
	}}
	return result
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

func annotateKnownCompatibilityDivergences(divergences []replayDivergence, cohorts []string) {
	usesSkip := slices.Contains(cohorts, "overlap_skip") || slices.Contains(cohorts, "overlap_unspecified")
	for index := range divergences {
		divergence := &divergences[index]
		if divergence.Classification == replayClassificationSignificant &&
			divergence.Kind == "action_request" &&
			slices.Equal(divergence.Fields, []string{"continued_failure"}) {
			divergence.Classification = replayClassificationKnownCompat
			divergence.KnownDifference = "terminal_failure_propagation"
			continue
		}
		if !usesSkip {
			continue
		}
		switch divergence.Kind {
		case "missing_action", "missing_action_attempt", "extra_action", "action_count", "completion_before_start", "unapplied_completion":
			divergence.Classification = replayClassificationKnownCompat
			divergence.KnownDifference = "skip_deadline_boundary"
		case "action_request":
			if divergence.Classification == replayClassificationSignificant && fieldsAreKnownSkipCascade(divergence.Fields) {
				divergence.Classification = replayClassificationKnownCompat
				divergence.KnownDifference = "skip_deadline_boundary"
			}
		default:
		}
	}
}

func fieldsAreKnownSkipCascade(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	for _, field := range fields {
		if field != "continued_failure" && field != "last_completion_result" {
			return false
		}
	}
	return true
}

func workflowTaskContainsUpdateBeforeStart(history *historypb.History, completedEventID int64) bool {
	if history == nil || completedEventID == 0 {
		return false
	}
	var scheduledEventID, startedEventID int64
	for _, event := range history.GetEvents() {
		if event.GetEventId() == completedEventID {
			attributes := event.GetWorkflowTaskCompletedEventAttributes()
			scheduledEventID = attributes.GetScheduledEventId()
			startedEventID = attributes.GetStartedEventId()
			break
		}
	}
	if scheduledEventID == 0 || startedEventID == 0 {
		return false
	}
	for _, event := range history.GetEvents() {
		if event.GetEventId() <= scheduledEventID || event.GetEventId() >= startedEventID {
			continue
		}
		if event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName() == legacyscheduler.SignalNameUpdate {
			return true
		}
	}
	return false
}

func TestWorkflowTaskContainsUpdateBeforeStart(t *testing.T) {
	history := &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{SignalName: legacyscheduler.SignalNameUpdate},
		}},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
			WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: 2, StartedEventId: 4},
		}},
	}}
	require.True(t, workflowTaskContainsUpdateBeforeStart(history, 5))
	require.False(t, workflowTaskContainsUpdateBeforeStart(history, 0))

	history.Events[1].EventId = 6
	require.False(t, workflowTaskContainsUpdateBeforeStart(history, 5))
}

func TestAnnotateKnownCompatibilityDivergences(t *testing.T) {
	divergences := []replayDivergence{
		{Classification: replayClassificationSignificant, Kind: "missing_action"},
		{Classification: replayClassificationInconclusive, Kind: "unapplied_completion"},
		{Classification: replayClassificationSignificant, Kind: "action_request", Fields: []string{"continued_failure"}},
		{Classification: replayClassificationSignificant, Kind: "action_request", Fields: []string{"input"}},
	}

	annotateKnownCompatibilityDivergences(divergences, []string{"overlap_unspecified"})
	require.Equal(t, replayClassificationKnownCompat, divergences[0].Classification)
	require.Equal(t, "skip_deadline_boundary", divergences[0].KnownDifference)
	require.Equal(t, replayClassificationKnownCompat, divergences[1].Classification)
	require.Equal(t, "skip_deadline_boundary", divergences[1].KnownDifference)
	require.Equal(t, replayClassificationKnownCompat, divergences[2].Classification)
	require.Equal(t, "terminal_failure_propagation", divergences[2].KnownDifference)
	require.Equal(t, replayClassificationSignificant, divergences[3].Classification)
}

func replayDivergenceKinds(result replayCaseResult) []string {
	kinds := make([]string, 0, len(result.Divergences))
	for _, divergence := range result.Divergences {
		kinds = append(kinds, divergence.Kind)
	}
	return kinds
}

func uniqueReplayDivergenceKinds(result replayCaseResult) []string {
	unique := make(map[string]struct{}, len(result.Divergences))
	for _, kind := range replayDivergenceKinds(result) {
		unique[kind] = struct{}{}
	}
	kinds := make([]string, 0, len(unique))
	for kind := range unique {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func replayDivergenceFields(result replayCaseResult, kind string) []string {
	unique := make(map[string]struct{})
	for _, divergence := range result.Divergences {
		if divergence.Kind == kind {
			for _, field := range divergence.Fields {
				unique[field] = struct{}{}
			}
		}
	}
	fields := make([]string, 0, len(unique))
	for field := range unique {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

func replayClassificationSeverity(classification replayClassification) int {
	switch classification {
	case replayClassificationUnsupported:
		return 5
	case replayClassificationSignificant:
		return 4
	case replayClassificationInconclusive:
		return 3
	case replayClassificationKnownCompat:
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

func writeReplayReport(path string, results []replayCaseResult, redact bool) error {
	return writeReplayReportWithCollections(path, results, nil, redact)
}

func writeReplayReportWithCollections(
	path string,
	results []replayCaseResult,
	collections []replayCollectionSummary,
	redact bool,
) error {
	reportResults := results
	if redact {
		reportResults = redactReplayResults(results)
		collections = redactCollectionSummaries(collections)
	}
	report := replayReport{
		Version:       4,
		Redacted:      redact,
		Summary:       make(map[string]int),
		CohortSummary: make(map[string]map[string]int),
		Collections:   collections,
		Cases:         reportResults,
	}
	for _, result := range results {
		report.Summary[string(result.Classification)]++
		for _, cohort := range result.Cohorts {
			if report.CohortSummary[cohort] == nil {
				report.CohortSummary[cohort] = make(map[string]int)
			}
			report.CohortSummary[cohort][string(result.Classification)]++
		}
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func readReplayCollectionSummaries(directory string) ([]replayCollectionSummary, error) {
	paths, err := filepath.Glob(filepath.Join(directory, "*", "collection-manifest.json"))
	if err != nil {
		return nil, err
	}
	summaries := make([]replayCollectionSummary, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var manifest replayCollectionManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return nil, fmt.Errorf("decode collection manifest %q: %w", path, err)
		}
		summary := replayCollectionSummary{
			Manifest: path, Namespace: manifest.Namespace, Seed: manifest.Seed,
			ServerVersion: manifest.ServerVersion, CollectorVersion: manifest.CollectorVersion,
			ListedPopulation: manifest.ListedPopulation, Inspected: manifest.Inspected,
			V1Inspected: manifest.V1Inspected, BaseSelected: manifest.BaseSelected,
			CollectionStatus: make(map[string]int),
		}
		for _, record := range manifest.Cases {
			summary.CollectionStatus[record.Status]++
		}
		selectedSchedules := make(map[string]struct{})
		for _, record := range manifest.Cases {
			if record.Status == "collected" {
				selectedSchedules[record.ScheduleID] = struct{}{}
			}
		}
		summary.SelectedSchedules = len(selectedSchedules)
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func redactCollectionSummaries(collections []replayCollectionSummary) []replayCollectionSummary {
	redacted := append([]replayCollectionSummary(nil), collections...)
	for index := range redacted {
		redacted[index].Manifest = replayOpaqueID(redacted[index].Manifest)
		redacted[index].Namespace = replayOpaqueID(redacted[index].Namespace)
	}
	return redacted
}

func redactReplayResults(results []replayCaseResult) []replayCaseResult {
	redacted := make([]replayCaseResult, len(results))
	for index, result := range results {
		redacted[index] = result
		redacted[index].Namespace = replayOpaqueID(result.Namespace)
		redacted[index].ScheduleID = replayOpaqueID(result.ScheduleID)
		redacted[index].History = replayOpaqueID(result.History)
		redacted[index].V1Starts = redactStrings(result.V1Starts)
		redacted[index].CHASMStarts = redactStrings(result.CHASMStarts)
		redacted[index].CHASMState.BufferedStarts = redactStrings(result.CHASMState.BufferedStarts)
		redacted[index].Divergences = append([]replayDivergence(nil), result.Divergences...)
		for divergenceIndex := range redacted[index].Divergences {
			divergence := &redacted[index].Divergences[divergenceIndex]
			divergence.WorkflowID = replayOpaqueID(divergence.WorkflowID)
			divergence.Message = "redacted " + divergence.Kind + " divergence"
			divergence.FieldDifferences = append([]replayFieldDifference(nil), divergence.FieldDifferences...)
			for fieldIndex := range divergence.FieldDifferences {
				divergence.FieldDifferences[fieldIndex].V1.Digest = ""
				divergence.FieldDifferences[fieldIndex].CHASM.Digest = ""
			}
		}
	}
	return redacted
}

func redactStrings(values []string) []string {
	redacted := make([]string, len(values))
	for index, value := range values {
		redacted[index] = replayOpaqueID(value)
	}
	return redacted
}

func replayOpaqueID(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("sha256:%x", sum[:12])
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

func drainCHASMTasks(engine *chasmtest.Engine, rootRef chasm.ComponentRef, now time.Time, budget *replayBudget) error {
	for range 1000 {
		pure, err := engine.FirePureTasks(rootRef, now)
		if err != nil {
			return err
		}
		sideEffect, err := engine.FireSideEffectTasks(rootRef, now)
		if err != nil {
			return err
		}
		if err := budget.addTasks(pure, sideEffect); err != nil {
			return err
		}
		if pure+sideEffect == 0 {
			return nil
		}
	}
	return errors.New("CHASM task drain did not converge")
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
	if schedule == nil {
		return
	}
	if schedule.GetState() == nil {
		schedule.State = &schedulepb.ScheduleState{}
	}
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
	executionMap workflowExecutionMap,
	strictWorkflowMapping bool,
	request *schedulespb.WatchWorkflowRequest,
	response *schedulespb.WatchWorkflowResponse,
) (bool, error) {
	if response.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return true, nil
	}
	observedExecution := workflowExecutionKey{
		workflowID: request.GetExecution().GetWorkflowId(),
		runID:      request.GetExecution().GetRunId(),
	}
	targetExecution, err := resolveObservedWorkflowExecution(executionMap, strictWorkflowMapping, observedExecution)
	if err != nil {
		var notStartedErr *workflowExecutionNotStartedError
		if errors.As(err, &notStartedErr) {
			return false, nil
		}
		return false, err
	}
	_, _, err = chasm.UpdateComponent(ctx, rootRef,
		func(s *scheduler.Scheduler, mutableCtx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker := s.Invoker.Get(mutableCtx)
			var requestID string
			matches := 0
			bufferedState := make([]string, 0, len(invoker.GetBufferedStarts()))
			for _, start := range invoker.GetBufferedStarts() {
				bufferedState = append(bufferedState, fmt.Sprintf(
					"%s(run=%s,completed=%t)", start.GetWorkflowId(), start.GetRunId(), start.GetCompleted() != nil,
				))
				if start.GetWorkflowId() == targetExecution.workflowID &&
					(targetExecution.runID == "" || start.GetRunId() == targetExecution.runID) {
					if start.GetCompleted() != nil {
						return struct{}{}, nil
					}
					matches++
					requestID = start.GetRequestId()
				}
			}
			if matches > 1 {
				return struct{}{}, fmt.Errorf(
					"running workflow %q completion matches multiple CHASM buffered starts",
					targetExecution.workflowID,
				)
			}
			if requestID == "" {
				return struct{}{}, fmt.Errorf(
					"running workflow %q run %q (V1 workflow %q run %q) not found in CHASM; buffered starts: %v",
					targetExecution.workflowID,
					targetExecution.runID,
					observedExecution.workflowID,
					observedExecution.runID,
					bufferedState,
				)
			}
			return struct{}{}, s.HandleNexusCompletion(mutableCtx, nexusCompletion(response, requestID))
		}, struct{}{})
	return err == nil, err
}

func replayCompletionErrorDivergence(
	err error,
	request *schedulespb.WatchWorkflowRequest,
	observedTime time.Time,
	v1Time bool,
) replayDivergence {
	divergence := replayDivergence{
		Classification: replayClassificationSignificant,
		Kind:           "completion_state",
		Message:        err.Error(),
		WorkflowID:     request.GetExecution().GetWorkflowId(),
	}
	var ambiguousErr *ambiguousWorkflowExecutionError
	if errors.As(err, &ambiguousErr) {
		divergence.Classification = replayClassificationInconclusive
		divergence.Kind = "ambiguous_completion"
	}
	if v1Time {
		divergence.V1Time = timePointer(observedTime)
	} else {
		divergence.CHASMTime = timePointer(observedTime)
	}
	return divergence
}

func seedCarriedWorkflowExecutions(executionMap workflowExecutionMap, starts []*schedulespb.BufferedStart) {
	for _, start := range starts {
		if start.GetRunId() == "" || start.GetCompleted() != nil {
			continue
		}
		execution := workflowExecutionKey{workflowID: start.GetWorkflowId(), runID: start.GetRunId()}
		executionMap[execution] = execution
	}
}

func resolveObservedWorkflowExecution(
	executionMap workflowExecutionMap,
	strict bool,
	observed workflowExecutionKey,
) (workflowExecutionKey, error) {
	execution, mapped := executionMap[observed]
	if mapped {
		return execution, nil
	}
	if observed.runID == "" {
		var match workflowExecutionKey
		matches := 0
		for source, target := range executionMap {
			if source.workflowID == observed.workflowID {
				match = target
				matches++
			}
		}
		if matches == 1 {
			return match, nil
		}
		if matches > 1 {
			return workflowExecutionKey{}, &ambiguousWorkflowExecutionError{workflowID: observed.workflowID}
		}
	}
	if !strict {
		return observed, nil
	}
	return workflowExecutionKey{}, &workflowExecutionNotStartedError{execution: observed}
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
		args:          &args,
		history:       history,
		watches:       make(map[int64]scheduledWatch),
		startsByEvent: make(map[int64]*schedulespb.StartWorkflowRequest),
		localWatches:  make(map[int64]observedWatchCompletion),
		startTime:     history.GetEvents()[0].GetEventTime().AsTime(),
		expectedSpec:  proto.CloneOf(args.GetSchedule()),
		searchAttrs:   started.GetSearchAttributes(),
		memo:          started.GetMemo(),
		baseActions:   args.GetInfo().GetActionCount(),
	}
	localActivities := captureV1LocalActivities(t, history)
	trace.capturedIDs = localActivities != nil
	for _, event := range history.GetEvents() {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_MARKER_RECORDED:
			metadata := localActivityMetadata(t, event)
			if metadata.ActivityType == "StartWorkflow" {
				if metadata.Attempt > 1 {
					trace.startRetries++
				}
				if event.GetMarkerRecordedEventAttributes().GetFailure() != nil {
					if localActivities == nil {
						trace.captureIssues++
						continue
					}
					request, ok := localActivities.startsByActivityID[metadata.ActivityID]
					if !ok {
						trace.captureIssues++
						continue
					}
					trace.failedStarts = append(trace.failedStarts, observedStartFailure{
						workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
						request: proto.CloneOf(request.GetRequest()),
					})
					trace.startAttempts = append(trace.startAttempts, observedStartAttempt{
						workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
						request: proto.CloneOf(request.GetRequest()), failed: true,
					})
					continue
				}
				var response schedulespb.StartWorkflowResponse
				details := event.GetMarkerRecordedEventAttributes().GetDetails()
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(details["result"], &response))
				start := observedStart{runID: response.GetRunId(), time: event.GetEventTime().AsTime()}
				if localActivities != nil {
					request, ok := localActivities.startsByActivityID[metadata.ActivityID]
					if !ok {
						trace.captureIssues++
						continue
					}
					start.workflowID = request.GetRequest().GetWorkflowId()
					start.request = proto.CloneOf(request.GetRequest())
				}
				trace.starts = append(trace.starts, start)
				trace.startAttempts = append(trace.startAttempts, observedStartAttempt{
					workflowID: start.workflowID, runID: start.runID, time: start.time,
					request: proto.CloneOf(start.request), workflowTaskCompletedEventID: event.GetMarkerRecordedEventAttributes().GetWorkflowTaskCompletedEventId(),
				})
			}
			if metadata.ActivityType == "WatchWorkflow" {
				if localActivities == nil {
					trace.captureIssues++
					continue
				}
				request, ok := localActivities.watchesByActivityID[metadata.ActivityID]
				if !ok {
					trace.captureIssues++
					continue
				}
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
			switch attributes.GetActivityType().GetName() {
			case "WatchWorkflow":
				var request schedulespb.WatchWorkflowRequest
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &request))
				trace.watches[event.GetEventId()] = scheduledWatch{request: &request}
			case "StartWorkflow":
				var request schedulespb.StartWorkflowRequest
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetInput(), &request))
				trace.startsByEvent[event.GetEventId()] = &request
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			attributes := event.GetActivityTaskCompletedEventAttributes()
			request, ok := trace.startsByEvent[attributes.GetScheduledEventId()]
			if !ok {
				continue
			}
			var response schedulespb.StartWorkflowResponse
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(attributes.GetResult(), &response))
			start := observedStart{
				workflowID: request.GetRequest().GetWorkflowId(), runID: response.GetRunId(),
				time: event.GetEventTime().AsTime(), request: proto.CloneOf(request.GetRequest()),
			}
			trace.starts = append(trace.starts, start)
			trace.startAttempts = append(trace.startAttempts, observedStartAttempt{
				workflowID: start.workflowID, runID: start.runID, time: start.time,
				request: proto.CloneOf(start.request),
			})
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			attributes := event.GetActivityTaskFailedEventAttributes()
			request, ok := trace.startsByEvent[attributes.GetScheduledEventId()]
			if ok {
				trace.failedStarts = append(trace.failedStarts, observedStartFailure{
					workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
					request: proto.CloneOf(request.GetRequest()),
				})
				trace.startAttempts = append(trace.startAttempts, observedStartAttempt{
					workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
					request: proto.CloneOf(request.GetRequest()), failed: true,
				})
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			attributes := event.GetActivityTaskTimedOutEventAttributes()
			request, ok := trace.startsByEvent[attributes.GetScheduledEventId()]
			if ok {
				trace.failedStarts = append(trace.failedStarts, observedStartFailure{
					workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
					request: proto.CloneOf(request.GetRequest()),
				})
				trace.startAttempts = append(trace.startAttempts, observedStartAttempt{
					workflowID: request.GetRequest().GetWorkflowId(), time: event.GetEventTime().AsTime(),
					request: proto.CloneOf(request.GetRequest()), failed: true,
				})
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
