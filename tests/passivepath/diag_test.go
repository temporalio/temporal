package passivepath

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
)

// dumpStalledWorkflow prints why a workflow is not progressing: its status, what it is
// waiting on, and the tail of its history. Used when the passive-path oracle fires, to
// separate "the refresher missed a task" from "the harness broke something".
func dumpStalledWorkflow(t *testing.T, c sdkclient.Client, workflowID, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	desc, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		t.Logf("DIAG describe failed: %v", err)
		return
	}
	info := desc.GetWorkflowExecutionInfo()
	t.Logf("DIAG status=%s historyLength=%d pendingActivities=%d pendingChildren=%d",
		info.GetStatus(), info.GetHistoryLength(),
		len(desc.GetPendingActivities()), len(desc.GetPendingChildren()))
	for _, a := range desc.GetPendingActivities() {
		t.Logf("DIAG pendingActivity id=%s type=%s state=%s attempt=%d lastStarted=%v",
			a.GetActivityId(), a.GetActivityType().GetName(), a.GetState(), a.GetAttempt(),
			a.GetLastStartedTime().AsTime())
	}
	if wt := desc.GetPendingWorkflowTask(); wt != nil {
		t.Logf("DIAG pendingWorkflowTask state=%s attempt=%d scheduledTime=%v",
			wt.GetState(), wt.GetAttempt(), wt.GetScheduledTime().AsTime())
	} else {
		t.Log("DIAG pendingWorkflowTask=<none>")
	}

	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []string
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			break
		}
		events = append(events, ev.GetEventType().String())
	}
	from := 0
	if len(events) > 25 {
		from = len(events) - 25
	}
	t.Logf("DIAG history tail (%d total): %v", len(events), events[from:])
}
