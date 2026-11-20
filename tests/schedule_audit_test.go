package tests

import (
	"io"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/tdbg/scheduleaudit"
)

type ScheduleAuditSuite struct {
	parallelsuite.Suite[*ScheduleAuditSuite]
}

func TestScheduleV1AuditVisibility(t *testing.T) {
	parallelsuite.Run(t, &ScheduleAuditSuite{}, v1ContextFactory)
}
func TestScheduleCHASMAuditVisibility(t *testing.T) {
	parallelsuite.Run(t, &ScheduleAuditSuite{}, chasmContextFactory)
}

func (s *ScheduleAuditSuite) TestVisibility(newContext contextFactory) {
	t := s.T()
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)
	ctx := newContext(testcore.NewContext())
	sid := testcore.RandomizeStr("audit-schedule")
	wid := testcore.RandomizeStr("audit-workflow")
	wt := testcore.RandomizeStr("audit-type")
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: wt})
	first := time.Now().UTC().Truncate(time.Second).Add(5 * time.Second)
	second := first.Add(5 * time.Second)
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:     &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{calendarSpec(first), calendarSpec(second)}},
		Action:   startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL},
	})
	loader := scheduleaudit.NewGRPCExecutionLoader(env.FrontendClient(), io.Discard, nil)
	var blocker scheduleaudit.Execution
	s.Await(func(s *ScheduleAuditSuite) {
		rows, err := loader.ListExecutions(newContext(s.Context()), env.Namespace().String(), []string{sid}, first.Add(-time.Second), first.Add(time.Second))
		s.NoError(err)
		s.Len(rows[sid], 1)
		blocker = rows[sid][0]
		s.Equal(first, blocker.NominalTime)
	}, 30*time.Second, pollInterval)
	s.AwaitTrue(func() bool { return time.Now().After(second.Add(time.Second)) }, 30*time.Second, pollInterval)
	rows, err := loader.ListExecutions(ctx, env.Namespace().String(), []string{sid}, second.Add(-time.Second), second.Add(time.Second))
	s.NoError(err)
	s.Len(rows[sid], 1)
	s.Equal(blocker.RunID, rows[sid][0].RunID, "pre-window running action remains visible")
	s.NoError(env.SdkClient().SignalWorkflow(ctx, blocker.WorkflowID, blocker.RunID, "finish", nil))
	var delayed scheduleaudit.Execution
	s.AwaitTrue(func() bool {
		rows, err := loader.ListExecutions(ctx, env.Namespace().String(), []string{sid}, second.Add(-time.Second), second.Add(time.Second))
		if err != nil || len(rows[sid]) != 2 {
			return false
		}
		for _, row := range rows[sid] {
			if row.NominalTime.Equal(second) {
				delayed = row
				return true
			}
		}
		return false
	}, 30*time.Second, pollInterval)
	s.True(delayed.StartTime.After(second.Add(time.Second)), "in-window nominal matches even when the buffered workflow starts after window end")
	s.NotEqual(blocker.RunID, delayed.RunID)
}
