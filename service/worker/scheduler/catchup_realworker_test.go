package scheduler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCatchupRealWorker runs the first task after a stall on a real SDK worker
// (real SideEffect/command handling) with the production logger and metrics
// handler, and reports the workflow task duration the server recorded.
//
//	TEMPORAL_PERF_ADDRESS=localhost:7233 go test ./service/worker/scheduler -run TestCatchupRealWorker -v
func TestCatchupRealWorker(t *testing.T) {
	addr := os.Getenv("TEMPORAL_PERF_ADDRESS")
	if addr == "" {
		t.Skip("set TEMPORAL_PERF_ADDRESS to a dev server")
	}
	withOneIteration(t)

	c, err := sdkclient.Dial(sdkclient.Options{
		HostPort:       addr,
		Logger:         prodLogger(t),
		MetricsHandler: prodMetrics(t),
		DataConverter:  sdk.PreferProtoDataConverter,
	})
	require.NoError(t, err)
	defer c.Close()

	tq := "catchup-perf-" + uuid.NewString()
	w := sdkworker.New(c, tq, sdkworker.Options{DeadlockDetectionTimeout: 5 * time.Minute})
	w.RegisterWorkflowWithOptions(SchedulerWorkflow, workflow.RegisterOptions{Name: WorkflowType})
	require.NoError(t, w.Start())
	defer w.Stop()

	for _, gap := range []time.Duration{time.Hour, 4*time.Hour + 24*time.Minute, 24 * time.Hour} {
		t.Run(gap.String(), func(t *testing.T) {
			args := newCatchupArgs(t, gap)
			args.State.LastProcessedTime = timestamppb.New(time.Now().Add(-gap))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			run, err := c.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID:                  "catchup-perf-" + uuid.NewString(),
				TaskQueue:           tq,
				WorkflowTaskTimeout: 120 * time.Second,
			}, WorkflowType, args)
			require.NoError(t, err)

			started, closed := firstWorkflowTask(ctx, t, c, run)
			d := closed.EventTime.AsTime().Sub(started.EventTime.AsTime())
			items := gap.Seconds()
			t.Logf("gap=%s items=%.0f wft=%s (%s) %.0f ns/item", gap, items, d,
				closed.EventType, float64(d.Nanoseconds())/items)
			_ = c.TerminateWorkflow(context.Background(), run.GetID(), run.GetRunID(), "perf test done")
		})
	}
}

// firstWorkflowTask waits for the first workflow task to close.
func firstWorkflowTask(ctx context.Context, t *testing.T, c sdkclient.Client, run sdkclient.WorkflowRun) (started, closed *historypb.HistoryEvent) {
	for {
		iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			e, err := iter.Next()
			require.NoError(t, err)
			switch e.EventType {
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
				if started == nil {
					started = e
				}
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
				if started != nil {
					return started, e
				}
			default:
			}
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, fmt.Sprintf("no closed workflow task: %v", ctx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
		started = nil
	}
}
