//go:build gen_replay_history

// This file generates testdata/replay_with_cron_accumulation.json.gz.
// Run it BEFORE applying the dedup fix (on branch test/schedule-cron-update-accumulation)
// against a local temporal dev server to produce a history with accumulated
// duplicate StructuredCalendar entries.
//
//	temporal server start-dev &
//	go test -run TestGenCronAccumulationHistory -v -tags gen_replay_history \
//	    go.temporal.io/server/service/worker/scheduler

package scheduler_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestGenCronAccumulationHistory produces testdata/replay_with_cron_accumulation.json.gz.
// This must be run with the pre-fix code so that duplicate StructuredCalendar entries
// persist in s.Schedule.Spec (old canonicalizeSpec appends without dedup, and old
// compileSpec has no GetVersion healing).
func TestGenCronAccumulationHistory(t *testing.T) {
	ctx := context.Background()

	c, err := client.Dial(client.Options{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	w := sdkworker.New(c, "temporal-sys-scheduler-gen", sdkworker.Options{})
	w.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{
		Name: scheduler.WorkflowType,
	})
	if err := w.Start(); err != nil {
		t.Fatalf("worker start: %v", err)
	}
	defer w.Stop()

	scheduleID := "gen-cron-dup"
	workflowID := scheduler.WorkflowIDPrefix + scheduleID

	_ = c.TerminateWorkflow(ctx, workflowID, "", "cleanup")
	time.Sleep(time.Second)

	args := &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				CronString: []string{"0 * * * *"},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "cron-dup-child",
						WorkflowType: &commonpb.WorkflowType{Name: "myworkflow"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		State: &schedulespb.InternalState{
			Namespace:   "default",
			NamespaceId: "default",
			ScheduleId:  scheduleID,
		},
	}

	_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "temporal-sys-scheduler-gen",
	}, scheduler.WorkflowType, args)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(2 * time.Second)

	// "0 * * * *" in compact Range form (as parseCronString would produce).
	hourly := &schedulepb.StructuredCalendarSpec{
		Second:     []*schedulepb.Range{{Start: 0}},
		Minute:     []*schedulepb.Range{{Start: 0}},
		Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
		DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
		Month:      []*schedulepb.Range{{Start: 1, End: 12}},
		DayOfWeek:  []*schedulepb.Range{{}},
	}

	// Inject 20 update signals directly (bypassing frontend), each adding one
	// more duplicate to the StructuredCalendar slice. After 20 signals the stored
	// spec has 20 identical entries (1 from initial compile + 19 from previous
	// signals already accumulated).
	for i := 0; i < 20; i++ {
		dups := make([]*schedulepb.StructuredCalendarSpec, i+1)
		for j := range dups {
			dups[j] = proto.Clone(hourly).(*schedulepb.StructuredCalendarSpec)
		}
		req := &schedulespb.FullUpdateRequest{
			Schedule: &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					StructuredCalendar: dups,
				},
				Action: args.Schedule.Action,
			},
			ConflictToken: int64(i + 1),
		}
		if err := c.SignalWorkflow(ctx, workflowID, "", scheduler.SignalNameUpdate, req); err != nil {
			t.Logf("signal %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("Sent 20 update signals with accumulating duplicates")
	time.Sleep(2 * time.Second)

	iter := c.GetWorkflowHistory(ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	type histJSON struct {
		Events []json.RawMessage `json:"events"`
	}
	var h histJSON
	for iter.HasNext() {
		evt, err := iter.Next()
		if err != nil {
			t.Fatalf("history iter: %v", err)
		}
		b, err := protojson.Marshal(evt)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		h.Events = append(h.Events, b)
	}
	t.Logf("Captured %d events", len(h.Events))

	out := "testdata/replay_with_cron_accumulation.json.gz"
	f, err := os.Create(out)
	if err != nil {
		t.Fatalf("create %s: %v", out, err)
	}
	gz := gzip.NewWriter(f)
	if err := json.NewEncoder(gz).Encode(h); err != nil {
		t.Fatalf("encode: %v", err)
	}
	_ = gz.Close()
	_ = f.Close()
	t.Logf("Written %s", out)
}
