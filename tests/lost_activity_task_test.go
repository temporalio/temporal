package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
)

const (
	lostTaskInjectedErr = "lost-activity-task-injected-failure"
	lostTaskActivityTQ  = "lost-activity-task-activity-tq"
	lostTaskGracePeriod = 30 * time.Second
)

func lostTaskControlPlaneOptions() workflow.ActivityOptions {
	opts := workerdeployment.DefaultActivityOptions
	opts.TaskQueue = lostTaskActivityTQ
	return opts
}

func lostTaskReleaseWindow() time.Duration {
	return workerdeployment.DefaultActivityOptions.ScheduleToCloseTimeout + lostTaskGracePeriod
}

type LostActivityTaskSuite struct {
	parallelsuite.Suite[*LostActivityTaskSuite]
}

func TestLostActivityTaskSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &LostActivityTaskSuite{}) //nolint:staticcheck // SA1019: needs a dedicated cluster with persistence fault injection.
}

type lostActivityTaskEnv struct {
	*testcore.TestEnv

	failEnqueue   atomic.Bool
	injectedCount atomic.Int32
	activityRuns  atomic.Int32

	createTasksMu   sync.Mutex
	createTasksSeen map[string]int
}

func (e *lostActivityTaskEnv) recordCreateTasks(tq string) {
	e.createTasksMu.Lock()
	defer e.createTasksMu.Unlock()
	if e.createTasksSeen == nil {
		e.createTasksSeen = map[string]int{}
	}
	e.createTasksSeen[tq]++
}

func (e *lostActivityTaskEnv) createTasksReport() map[string]int {
	e.createTasksMu.Lock()
	defer e.createTasksMu.Unlock()
	out := make(map[string]int, len(e.createTasksSeen))
	for k, v := range e.createTasksSeen {
		out[k] = v
	}
	return out
}

func (e *lostActivityTaskEnv) countingActivity(context.Context) error {
	e.activityRuns.Add(1)
	return nil
}

func (e *lostActivityTaskEnv) controlPlaneWorkflow(ctx workflow.Context) error {
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, lostTaskControlPlaneOptions()), e.countingActivity,
	).Get(ctx, nil)
}

func (e *lostActivityTaskEnv) stillRunning(ctx context.Context, run sdkclient.WorkflowRun) bool {
	desc, err := e.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	if err != nil {
		return true
	}
	return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

func (s *LostActivityTaskSuite) newTestEnv() *lostActivityTaskEnv {
	e := &lostActivityTaskEnv{}
	e.TestEnv = testcore.NewEnv(s.T(),
		testcore.WithPersistenceFaultInjection(&config.FaultInjection{
			Injector: func(target config.FaultInjectionTarget) error {
				if target.Store != config.TaskStoreName || target.Method != "CreateTasks" {
					return nil
				}
				req, ok := target.Request.(*persistence.InternalCreateTasksRequest)
				if !ok {
					return nil
				}
				e.recordCreateTasks(req.TaskQueue)
				if !strings.Contains(req.TaskQueue, lostTaskActivityTQ) {
					return nil
				}
				if !e.failEnqueue.Load() {
					return nil
				}
				e.injectedCount.Add(1)
				return errors.New(lostTaskInjectedErr)
			},
		}),
		testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQEnabled, true),
		testcore.WithDynamicConfig(dynamicconfig.HistoryTaskDLQErrorPattern, lostTaskInjectedErr),
	)
	return e
}

func (s *LostActivityTaskSuite) startRun(
	env *lostActivityTaskEnv, wf any, idPrefix string,
) sdkclient.WorkflowRun {
	env.SdkWorker().RegisterWorkflow(wf)
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        idPrefix + uuid.NewString(),
		TaskQueue: env.WorkerTaskQueue(),
	}, wf)
	s.NoError(err)
	return run
}

func (s *LostActivityTaskSuite) awaitActivityLost(env *lostActivityTaskEnv, run sdkclient.WorkflowRun) {
	await.Require(s.Context(), s.T(), func(t *await.T) {
		require.Positive(t, env.injectedCount.Load(),
			"fault was never injected; CreateTasks calls seen: %v", env.createTasksReport())
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.GetPendingActivities(), 1, "expected exactly one pending activity")
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			desc.GetPendingActivities()[0].GetState(), "activity should be SCHEDULED, never started")
	}, 30*time.Second, 200*time.Millisecond)
	s.Zero(env.activityRuns.Load(), "activity must never have executed")
}

func (s *LostActivityTaskSuite) TestLostActivityTask_ControlPlaneOptionsReleaseCaller() {
	testcontext.For(s.T(), testcontext.WithTimeout(lostTaskReleaseWindow()+90*time.Second))

	env := s.newTestEnv()
	env.failEnqueue.Store(true)

	run := s.startRun(env, env.controlPlaneWorkflow, "lost-activity-control-plane-")
	s.awaitActivityLost(env, run)

	env.failEnqueue.Store(false)
	w := sdkworker.New(env.SdkClient(), lostTaskActivityTQ, sdkworker.Options{})
	w.RegisterActivity(env.countingActivity)
	s.NoError(w.Start())
	defer w.Stop()

	s.Eventually(func() bool { return !env.stillRunning(s.Context(), run) },
		lostTaskReleaseWindow(), time.Second,
		"the activity task was lost and %v bounds no end-to-end deadline, so nothing fails "+
			"the never-started attempt, the retry policy cannot advance, and the caller waits "+
			"forever. Set ScheduleToCloseTimeout on DefaultActivityOptions in "+
			"service/worker/workerdeployment/util.go.",
		workerdeployment.DefaultActivityOptions)

	s.Zero(env.activityRuns.Load(), "activity must never have executed")
}
