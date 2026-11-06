package scheduler

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestGenerateChunkedCatchupFixtures records the ChunkedCatchup (v15) replay fixtures by
// running this binary's scheduler workflow on a real worker against a dev server:
//
//	temporal server start-dev --port 7255 --headless
//	SCHEDULER_FIXTURE_ADDRESS=localhost:7255 go test ./service/worker/scheduler \
//	  -run TestGenerateChunkedCatchupFixtures -count 1 -v
//
// The schedule starts 1h+1200s behind with a 1h catchup window and 500 matches per task,
// so it walks 500, 500, then 200 misses before the Skip shortcut and a start:
//   - replay_chunked_catchup_v15: the first run (a chunk of misses, then continue-as-new)
//   - replay_chunked_catchup_skip_v15: the last run (rest of the misses, Skip shortcut,
//     start, sleep)
func TestGenerateChunkedCatchupFixtures(t *testing.T) {
	addr := os.Getenv("SCHEDULER_FIXTURE_ADDRESS")
	if addr == "" {
		t.Skip("set SCHEDULER_FIXTURE_ADDRESS to a dev server to regenerate the fixtures")
	}
	prev := CurrentTweakablePolicies
	CurrentTweakablePolicies.Version = ChunkedCatchup
	CurrentTweakablePolicies.MaxCatchupMatchesPerTask = 500
	t.Cleanup(func() { CurrentTweakablePolicies = prev })

	c, err := sdkclient.Dial(sdkclient.Options{HostPort: addr, DataConverter: sdk.PreferProtoDataConverter})
	require.NoError(t, err)
	defer c.Close()

	tq := "scheduler-fixture-" + uuid.NewString()
	w := sdkworker.New(c, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(SchedulerWorkflow, workflow.RegisterOptions{Name: WorkflowType})
	registerStubActivities(w)
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	args := newCatchupArgs(t, 0)
	args.Schedule.Action.GetStartWorkflow().TaskQueue.Name = tq
	args.Schedule.Policies.CatchupWindow = durationpb.New(time.Hour)
	args.State.LastProcessedTime = timestamppb.New(time.Now().UTC().Truncate(time.Second).Add(-time.Hour - 1200*time.Second))
	run, err := c.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        WorkflowIDPrefix + "fixture-chunked-" + uuid.NewString(),
		TaskQueue: tq,
	}, WorkflowType, args)
	require.NoError(t, err)
	defer func() { _ = c.TerminateWorkflow(context.Background(), run.GetID(), "", "fixture recorded") }()

	// Follow the continue-as-new chain to the run that started the action and slept.
	var runs []*historypb.History
	runID := run.GetRunID()
	for {
		h := waitForHistory(ctx, t, c, run.GetID(), runID, func(h *historypb.History) bool {
			return continuedAsNewTo(h) != "" || (hasEvent(h, enumspb.EVENT_TYPE_TIMER_STARTED) && hasMarker(h, "LocalActivity"))
		})
		runs = append(runs, h)
		next := continuedAsNewTo(h)
		if next == "" {
			break
		}
		runID = next
		require.Less(t, len(runs), 10, "catch-up did not converge")
	}
	require.Len(t, runs, 3, "expected two chunks of misses, then the run that starts")

	writeFixture(t, "replay_chunked_catchup_v15.json.gz", runs[0])
	writeFixture(t, "replay_chunked_catchup_skip_v15.json.gz", runs[len(runs)-1])
}

// TestGeneratePreV15CatchupFixtures records catch-ups after a stall at the default version
// (below ChunkedCatchup), which walk the whole range in one task:
//   - replay_catchup_stall_skip_v12: 1h+1200s behind, 1h window, Skip (1200 misses, a start,
//     the rest buffered and dropped), then a pause signal
//   - replay_catchup_stall_buffer_all_v12: 1800s behind, BufferAll (a full buffer, 800 dropped)
//
// The checked-in fixtures were recorded from main (8fcaa51f02), before ChunkedCatchup existed:
// TestReplays replaying them on this binary checks that the pre-v15 walk, and receiving
// signals, are unchanged. To re-record from main, copy this function, the helpers below and
// catchup_helpers_test.go into a main checkout, and run it as TestGenerateChunkedCatchupFixtures
// is run.
func TestGeneratePreV15CatchupFixtures(t *testing.T) {
	addr := os.Getenv("SCHEDULER_FIXTURE_ADDRESS")
	if addr == "" {
		t.Skip("set SCHEDULER_FIXTURE_ADDRESS to a dev server to regenerate the fixtures")
	}
	c, err := sdkclient.Dial(sdkclient.Options{HostPort: addr, DataConverter: sdk.PreferProtoDataConverter})
	require.NoError(t, err)
	defer c.Close()

	tq := "scheduler-fixture-" + uuid.NewString()
	w := sdkworker.New(c, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(SchedulerWorkflow, workflow.RegisterOptions{Name: WorkflowType})
	registerStubActivities(w)
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	record := func(name string, gap time.Duration, policies *schedulepb.SchedulePolicies, pause bool) {
		args := newCatchupArgs(t, 0)
		args.Schedule.Action.GetStartWorkflow().TaskQueue.Name = tq
		args.Schedule.Policies = policies
		args.State.LastProcessedTime = timestamppb.New(time.Now().UTC().Truncate(time.Second).Add(-gap))
		run, err := c.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        WorkflowIDPrefix + "fixture-stall-" + uuid.NewString(),
			TaskQueue: tq,
		}, WorkflowType, args)
		require.NoError(t, err)
		defer func() { _ = c.TerminateWorkflow(context.Background(), run.GetID(), "", "fixture recorded") }()

		started := func(h *historypb.History) bool {
			return hasEvent(h, enumspb.EVENT_TYPE_TIMER_STARTED) && hasMarker(h, "LocalActivity")
		}
		h := waitForHistory(ctx, t, c, run.GetID(), run.GetRunID(), started)
		require.Empty(t, continuedAsNewTo(h), "the walk finished in one run")
		if pause {
			require.NoError(t, c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), SignalNamePatch,
				&schedulepb.SchedulePatch{Pause: "paused after a stall"}))
			// Wait for the task that handled the signal.
			h = waitForHistory(ctx, t, c, run.GetID(), run.GetRunID(), func(h *historypb.History) bool {
				signaled := false
				for _, e := range h.Events {
					switch e.EventType {
					case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
						signaled = true
					case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
						if signaled {
							return true
						}
					default:
					}
				}
				return false
			})
		}
		writeFixture(t, name, h)
	}
	record("replay_catchup_stall_skip_v12.json.gz", time.Hour+1200*time.Second, &schedulepb.SchedulePolicies{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow: durationpb.New(time.Hour),
	}, true)
	record("replay_catchup_stall_buffer_all_v12.json.gz", 1800*time.Second, &schedulepb.SchedulePolicies{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	}, false)
}

func registerStubActivities(w sdkworker.Worker) {
	w.RegisterActivityWithOptions(func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		return &schedulespb.StartWorkflowResponse{RunId: uuid.NewString(), RealStartTime: timestamppb.Now()}, nil
	}, activity.RegisterOptions{Name: "StartWorkflow"})
	w.RegisterActivityWithOptions(func(ctx context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		if req.LongPoll {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}, activity.RegisterOptions{Name: "WatchWorkflow"})
	w.RegisterActivityWithOptions(func(context.Context, *schedulespb.CancelWorkflowRequest) error { return nil },
		activity.RegisterOptions{Name: "CancelWorkflow"})
	w.RegisterActivityWithOptions(func(context.Context, *schedulespb.TerminateWorkflowRequest) error { return nil },
		activity.RegisterOptions{Name: "TerminateWorkflow"})
}

func waitForHistory(ctx context.Context, t *testing.T, c sdkclient.Client, id, runID string, done func(*historypb.History) bool) *historypb.History {
	for {
		h := &historypb.History{}
		iter := c.GetWorkflowHistory(ctx, id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			e, err := iter.Next()
			require.NoError(t, err)
			h.Events = append(h.Events, e)
		}
		if done(h) {
			return h
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out waiting for run "+runID)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func continuedAsNewTo(h *historypb.History) string {
	for _, e := range h.Events {
		if a := e.GetWorkflowExecutionContinuedAsNewEventAttributes(); a != nil {
			return a.NewExecutionRunId
		}
	}
	return ""
}

func hasEvent(h *historypb.History, typ enumspb.EventType) bool {
	for _, e := range h.Events {
		if e.EventType == typ {
			return true
		}
	}
	return false
}

func hasMarker(h *historypb.History, name string) bool {
	for _, e := range h.Events {
		if e.GetMarkerRecordedEventAttributes().GetMarkerName() == name {
			return true
		}
	}
	return false
}

func writeFixture(t *testing.T, name string, h *historypb.History) {
	data, err := protojson.Marshal(h)
	require.NoError(t, err)
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)
	_, err = zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	require.NoError(t, os.WriteFile(filepath.Join("testdata", name), buf.Bytes(), 0o644))
	t.Logf("wrote testdata/%s (%d events)", name, len(h.Events))
}
