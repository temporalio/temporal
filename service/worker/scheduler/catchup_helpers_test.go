package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Shape of the production schedules that hit the catch-up death spiral: an
// every-second calendar plus a far-future "keep-alive" calendar, SKIP, and a
// one-year catchup window.
const catchupSpecJSON = `{
  "structuredCalendar": [
    {"second": [{"start": 59, "end": 59}], "minute": [{"start": 59, "end": 59}], "hour": [{"start": 23, "end": 23}],
     "dayOfMonth": [{"start": 31, "end": 31}], "month": [{"start": 12, "end": 12}], "year": [{"start": 2100, "end": 2100}],
     "dayOfWeek": [{"end": 6, "step": 1}], "comment": "keep-alive"},
    {"second": [{"end": 59}], "minute": [{"end": 59}], "hour": [{"end": 23}],
     "dayOfMonth": [{"start": 1, "end": 31}], "month": [{"start": 1, "end": 12}], "dayOfWeek": [{"end": 6}]}
  ],
  "startTime": "1970-01-01T00:00:00Z",
  "endTime": "2124-11-03T06:25:23.869Z",
  "timezoneName": "Etc/GMT"
}`

// newCatchupArgs builds StartScheduleArgs shaped like the production ones (as
// after continue-as-new), with lastProcessedTime gap before baseStartTime.
func newCatchupArgs(tb testing.TB, gap time.Duration) *schedulespb.StartScheduleArgs {
	spec := &schedulepb.ScheduleSpec{}
	require.NoError(tb, protojson.Unmarshal([]byte(catchupSpecJSON), spec))
	jsonPayload := func(s string) *commonpb.Payload {
		return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(s)}
	}
	recent := make([]*schedulepb.ScheduleActionResult, 10)
	for i := range recent {
		t := baseStartTime.Add(-gap - time.Duration(10-i)*2*time.Minute)
		recent[i] = &schedulepb.ScheduleActionResult{
			ScheduleTime: timestamppb.New(t), ActualTime: timestamppb.New(t),
			StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "myid-" + t.Format(time.RFC3339), RunId: "r"},
		}
	}
	return &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: spec,
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   "myid",
				WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
					jsonPayload(`{"definition":"` + strings.Repeat("x", 8000) + `"}`),
					{Metadata: map[string][]byte{"encoding": []byte("binary/null")}},
					jsonPayload(`"` + strings.Repeat("y", 36) + `"`),
					jsonPayload(`"` + strings.Repeat("z", 93) + `"`),
				}},
				WorkflowRunTimeout: durationpb.New(720 * time.Hour),
				RetryPolicy:        &commonpb.RetryPolicy{MaximumAttempts: 1},
			}}},
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				CatchupWindow: durationpb.New(365 * 24 * time.Hour),
			},
		},
		Info: &schedulepb.ScheduleInfo{
			ActionCount:    450000,
			OverlapSkipped: 55000000,
			RecentActions:  recent,
			CreateTime:     timestamppb.New(baseStartTime.Add(-365 * 24 * time.Hour)),
		},
		State: &schedulespb.InternalState{
			Namespace:         "myns",
			NamespaceId:       "mynsid",
			ScheduleId:        "myschedule",
			ConflictToken:     InitialConflictToken,
			LastProcessedTime: timestamppb.New(baseStartTime.Add(-gap)),
		},
	}
}

// withOneIteration makes every run continue-as-new after one iteration, so a test sees
// each chunk of a catch-up as its own run.
func withOneIteration(tb testing.TB) {
	prev := CurrentTweakablePolicies.IterationsBeforeContinueAsNew
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
	tb.Cleanup(func() { CurrentTweakablePolicies.IterationsBeforeContinueAsNew = prev })
}
