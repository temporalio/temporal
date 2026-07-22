package scheduler_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	"pgregory.net/rapid"
)

type schedulerDurableTrace struct {
	Version int      `json:"version"`
	Name    string   `json:"name"`
	Steps   []string `json:"steps"`
}

func TestSchedulerDurableTraceCorpus(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("testdata/durable_traces/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		var trace schedulerDurableTrace
		require.NoError(t, json.Unmarshal(contents, &trace))
		t.Run(trace.Name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				runSchedulerDurableTrace(t, trace)
			})
		})
	}
}

// TestSchedulerDurableTraceRapid uses the same runner as the fixed corpus so
// generated campaigns and minimized regressions share one trace vocabulary.
func TestSchedulerDurableTraceRapid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		t.Repeat(map[string]func(*rapid.T){
			"durable trace": func(t *rapid.T) {
				trace := schedulerDurableTrace{
					Version: 1,
					Name:    "generated-lost-response-retry",
					Steps: []string{
						"trigger", "deliver_execute", "stale_redelivery", "reload", "advance_retry", "verify",
					},
				}
				runSchedulerDurableTrace(t, trace)
			},
		})
	})
}

func runSchedulerDurableTrace(t *rapid.T, trace schedulerDurableTrace) {
	t.Helper()
	if trace.Version != 1 {
		t.Fatalf("unsupported durable trace version %d", trace.Version)
	}
	env := newSchedulerModelEnv(t, defaultModelEnvConfig())
	env.workflows.pushCommittedResponseLost(context.DeadlineExceeded)
	env.workflows.pushReconcileCommittedStart()

	var executeTask any
	for _, step := range trace.Steps {
		switch step {
		case "trigger":
			_, err := env.handler.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.PatchScheduleRequest{
					Namespace:  namespace,
					ScheduleId: env.scheduleID,
					Patch:      &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{}},
				},
			})
			mustNoError(t, err)
		case "deliver_execute":
			task, result := env.executeOne(t)
			if result.Dropped {
				t.Fatal("lost-response execute task was dropped")
			}
			executeTask = task
		case "stale_redelivery":
			task, ok := executeTask.(tasks.Task)
			if !ok {
				t.Fatal("missing execute task for stale redelivery")
			}
			result := env.redeliver(t, task)
			if !result.Dropped || result.Executed != 0 {
				t.Fatalf("stale redelivery result: %+v", result)
			}
		case "reload":
			env.reload(t)
		case "advance_retry":
			state := env.internal(t)
			if len(state.buffered) != 1 || state.buffered[0].backoffTime.IsZero() {
				t.Fatalf("expected one retry-pending buffered start: %+v", state.buffered)
			}
			env.timeSource.Update(state.buffered[0].backoffTime)
			env.drain(t)
		case "verify":
			workflows := env.workflows.snapshot()
			if len(workflows.starts) != 1 || len(workflows.startCalls) != 2 {
				t.Fatalf("external start commits/calls: %d/%d", len(workflows.starts), len(workflows.startCalls))
			}
			state := env.internal(t)
			if len(state.buffered) != 1 || state.buffered[0].runID == "" {
				t.Fatalf("expected one internally tracked running action: %+v", state.buffered)
			}
			if got := env.describe(t).GetInfo().GetActionCount(); got != 1 {
				t.Fatalf("action count: got %d, want 1", got)
			}
		default:
			t.Fatalf("unknown durable trace step %q", step)
		}
	}
}
