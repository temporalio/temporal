package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type TimeSkippingPropagationTestSuite struct {
	parallelsuite.Suite[*TimeSkippingPropagationTestSuite]
}

func TestTimeSkippingPropagationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingPropagationTestSuite{})
}

// TestTimeSkippingPropagation_ParentTimerChildTimerParentTimer exercises end-to-end
// propagation of TimeSkippingConfig + PropagatedSkippedDuration from parent to child,
// together with the idle-only skipping invariant on the parent.
//
// Scenario:
//   - Parent starts with TimeSkippingConfig{Enabled: true}.
//   - Parent issues StartTimer(t1, 1h). Parent is idle on t1 → server skips 1h.
//     After skip: parent.AccumulatedSkippedDuration = 1h.
//   - t1 fires. Parent issues StartChildWorkflow(child) and StartTimer(t2, 1h).
//     Parent now has a pending child → NOT idle → parent does NOT skip.
//   - Child is started. Its mutable state receives TimeSkippingConfig propagated
//     from the parent, plus PropagatedSkippedDuration = 1h (parent's accumulated
//     skip at the moment the child started).
//   - Child issues StartTimer(tc, 3h). Child is idle → server skips 3h.
//     After skip: child.AccumulatedSkippedDuration = 3h.
//   - tc fires. Child completes.
//   - Parent receives ChildWorkflowExecutionCompleted. Parent is now only waiting
//     on t2 → idle → server skips 1h to fire t2.
//     After skip: parent.AccumulatedSkippedDuration = 2h.
//   - t2 fires. Parent completes.
//
// End-state assertions:
//   - parent.AccumulatedSkippedDuration == 2h.
//   - child.TimeSkippingInfo.Config.Enabled == true (propagated).
//   - child.TimeSkippingInfo.Config.PropagatedSkippedDuration == 1h.
//   - child.AccumulatedSkippedDuration == 3h.
//   - child's total virtual advance (propagated + accumulated) == 4h.
//   - Wall-clock elapsed is nowhere near 5h of virtual time.
func (s *TimeSkippingPropagationTestSuite) TestTimeSkippingPropagation_ParentTimerChildTimerParentTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childWFID := parentWFID + "-child"

	startWall := time.Now()

	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	ns := env.Namespace().String()
	tq := tv.TaskQueue()

	parentStarted, parentKickedOff, parentDone := false, false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(timerCmd("t1", time.Hour)), nil
		}
		if fired["t1"] && !parentKickedOff {
			parentKickedOff = true
			return cmdsResponse(
				childCmd(ns, childWFID, childWFType, tq),
				timerCmd("t2", time.Hour),
			), nil
		}
		if fired["t2"] && !parentDone {
			parentDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	childStarted, childDone := false, false
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !childStarted {
			childStarted = true
			return cmdsResponse(timerCmd("tc", 3*time.Hour)), nil
		}
		if fired["tc"] && !childDone {
			childDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	s.drivePollsUntilClosed(env, ctx, tv, dispatch, parentWFID, parentRunID, 20)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 5h of virtual time", elapsed)

	// ---- Parent ----
	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, parentMS.State.ExecutionState.State)
	parentTSI := parentMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(parentTSI)
	s.approxDuration(2*time.Hour, parentTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"parent should accumulate 1h (t1) + 1h (t2 after child) = 2h")

	// ---- Child ----
	childMS := s.getMutableStateByID(env, ctx, childWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State)
	childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(childTSI)
	childCfg := childTSI.GetConfig()
	s.NotNil(childCfg)
	s.True(childCfg.GetEnabled(), "child TSC.Enabled propagated from parent")
	s.approxDuration(time.Hour, childCfg.GetPropagatedSkippedDuration().AsDuration(),
		"child PropagatedSkippedDuration == parent's AccumulatedSkippedDuration at child-start (1h)")
	s.approxDuration(3*time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"child accumulated 3h from tc")
	s.approxDuration(4*time.Hour,
		childCfg.GetPropagatedSkippedDuration().AsDuration()+childTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"child virtual ends 4h ahead (1h propagated + 3h accumulated)")

	// The parent's StartChildWorkflowExecutionInitiated event must carry the propagated
	// TimeSkippingConfig — this is what the transfer task consumes when it starts the child.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 1)
	initTSC := initEvents[0].GetStartChildWorkflowExecutionInitiatedEventAttributes().GetTimeSkippingConfig()
	s.NotNil(initTSC, "initiated event should carry TimeSkippingConfig snapshot")
	s.True(initTSC.GetEnabled(), "snapshot mirrors parent's Enabled flag")
	s.approxDuration(time.Hour, initTSC.GetPropagatedSkippedDuration().AsDuration(),
		"snapshot PSD == parent's AccumulatedSkippedDuration at command time (1h)")
}

// TestTimeSkippingPropagation_ParentWithTwoChildren verifies that:
//   - All children started in the same WFT inherit the parent's TimeSkippingConfig and
//     the parent's AccumulatedSkippedDuration at the moment they start.
//   - The parent is blocked from skipping while ANY child is pending, and resumes
//     skipping only after all children complete (idle-only invariant).
//
// Scenario:
//   - Parent TSC enabled. Parent issues StartTimer(t1, 1h) → skips 1h.
//   - Parent issues [StartChild(c1), StartChild(c2), StartTimer(t2, 1h)].
//     Each child inherits TSC and receives PropagatedSkippedDuration = 1h.
//   - c1 issues StartTimer(tc1, 2h) → idle → skips 2h → completes.
//   - c2 issues StartTimer(tc2, 2h) → idle → skips 2h → completes.
//   - Only after BOTH c1 and c2 complete does parent become idle on t2 → skips 1h → t2 fires.
//   - Parent completes.
func (s *TimeSkippingPropagationTestSuite) TestTimeSkippingPropagation_ParentWithTwoChildren() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	child1WFID := parentWFID + "-child-1"
	child2WFID := parentWFID + "-child-2"

	startWall := time.Now()

	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	ns := env.Namespace().String()
	tq := tv.TaskQueue()

	parentStarted, parentKickedOff, parentDone := false, false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(timerCmd("t1", time.Hour)), nil
		}
		if fired["t1"] && !parentKickedOff {
			parentKickedOff = true
			return cmdsResponse(
				childCmd(ns, child1WFID, childWFType, tq),
				childCmd(ns, child2WFID, childWFType, tq),
				timerCmd("t2", time.Hour),
			), nil
		}
		if fired["t2"] && !parentDone {
			parentDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	// Child handler keyed by WorkflowID so the two children track state independently.
	childStarted := map[string]bool{}
	childDone := map[string]bool{}
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		id := task.WorkflowExecution.WorkflowId
		fired := firedTimers(task)
		if !childStarted[id] {
			childStarted[id] = true
			return cmdsResponse(timerCmd("tc", 2*time.Hour)), nil
		}
		if fired["tc"] && !childDone[id] {
			childDone[id] = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	s.drivePollsUntilClosed(env, ctx, tv, dispatch, parentWFID, parentRunID, 30)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than virtual time", elapsed)

	// ---- Parent ----
	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, parentMS.State.ExecutionState.State)
	s.approxDuration(2*time.Hour, parentMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration(),
		"parent skip: 1h (t1) + 1h (t2 after BOTH children complete) = 2h")

	// ---- Each child inherits identically ----
	for _, id := range []string{child1WFID, child2WFID} {
		childMS := s.getMutableStateByID(env, ctx, id)
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State, "%s", id)
		childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
		s.NotNil(childTSI, "%s TSI", id)
		s.True(childTSI.GetConfig().GetEnabled(), "%s inherited Enabled", id)
		s.approxDuration(time.Hour, childTSI.GetConfig().GetPropagatedSkippedDuration().AsDuration(),
			"%s PropagatedSkippedDuration == parent's accumulated at child-start (1h)", id)
		s.approxDuration(2*time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
			"%s accumulated 2h from its own timer", id)
	}

	// Both children are initiated in the same WFT — the parent's history must carry two
	// StartChildWorkflowExecutionInitiated events, each with the snapshot (Enabled=true,
	// PSD=1h) computed from parent state at command time.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 2)
	for _, e := range initEvents {
		tsc := e.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetTimeSkippingConfig()
		s.NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.approxDuration(time.Hour, tsc.GetPropagatedSkippedDuration().AsDuration())
	}
}

// TestTimeSkippingPropagation_ThreeGenerations verifies that propagation only carries
// the *direct* parent's accumulated skip — grandparent contributions are NOT re-added
// transitively (they are already folded into the direct parent's accumulated skip before
// the direct parent started its own child).
//
// Scenario:
//   - Grandparent (G) TSC enabled. G: StartTimer(gt1, 1h) → skips 1h → G.accum = 1h.
//   - G: StartChild(P) + StartTimer(gt2, 1h). P inherits PropagatedSkippedDuration = 1h.
//   - P: StartTimer(pt1, 2h) → skips 2h → P.accum = 2h.
//   - P: StartChild(C) + StartTimer(pt2, 1h). C inherits PropagatedSkippedDuration = 2h
//     (= P's accumulated at time of C start, NOT 1h + 2h = 3h).
//   - C: StartTimer(ct, 1h) → skips 1h → C.accum = 1h → completes.
//   - P: idle on pt2 → skips 1h → P.accum = 3h. pt2 fires. P completes.
//   - G: idle on gt2 → skips 1h → G.accum = 2h. gt2 fires. G completes.
//
// Final state: C.config.Propagated == 2h (direct parent only); C.accum == 1h.
func (s *TimeSkippingPropagationTestSuite) TestTimeSkippingPropagation_ThreeGenerations() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	gWFType := &commonpb.WorkflowType{Name: tv.WorkflowType().Name + "-g"}
	pWFType := &commonpb.WorkflowType{Name: tv.WorkflowType().Name + "-p"}
	cWFType := &commonpb.WorkflowType{Name: tv.WorkflowType().Name + "-c"}
	gWFID := tv.WorkflowID() + "-g"
	pWFID := tv.WorkflowID() + "-p"
	cWFID := tv.WorkflowID() + "-c"

	startWall := time.Now()

	// Start grandparent with TSC enabled. We start with gWFType (not tv's default) so
	// dispatch can route G's tasks distinctly.
	gStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          gWFID,
		WorkflowType:        gWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	gRunID := gStart.RunId

	ns := env.Namespace().String()
	tq := tv.TaskQueue()

	gStarted, gKickedOff, gDone := false, false, false
	gHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !gStarted {
			gStarted = true
			return cmdsResponse(timerCmd("gt1", time.Hour)), nil
		}
		if fired["gt1"] && !gKickedOff {
			gKickedOff = true
			return cmdsResponse(
				childCmd(ns, pWFID, pWFType, tq),
				timerCmd("gt2", time.Hour),
			), nil
		}
		if fired["gt2"] && !gDone {
			gDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	pStarted, pKickedOff, pDone := false, false, false
	pHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !pStarted {
			pStarted = true
			return cmdsResponse(timerCmd("pt1", 2*time.Hour)), nil
		}
		if fired["pt1"] && !pKickedOff {
			pKickedOff = true
			return cmdsResponse(
				childCmd(ns, cWFID, cWFType, tq),
				timerCmd("pt2", time.Hour),
			), nil
		}
		if fired["pt2"] && !pDone {
			pDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	cStarted, cDone := false, false
	cHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !cStarted {
			cStarted = true
			return cmdsResponse(timerCmd("ct", time.Hour)), nil
		}
		if fired["ct"] && !cDone {
			cDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	dispatch := typeDispatch(map[string]wftHandler{
		gWFType.Name: gHandler,
		pWFType.Name: pHandler,
		cWFType.Name: cHandler,
	})

	s.drivePollsUntilClosed(env, ctx, tv, dispatch, gWFID, gRunID, 40)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 3*time.Minute, "wall-clock elapsed (%s) should be far less than virtual time", elapsed)

	// ---- Grandparent ----
	gMS := s.getMutableState(env, gWFID, gRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, gMS.State.ExecutionState.State)
	s.approxDuration(2*time.Hour, gMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration(),
		"G: 1h (gt1) + 1h (gt2 after P completes) = 2h")

	// ---- Parent ----
	pMS := s.getMutableStateByID(env, ctx, pWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, pMS.State.ExecutionState.State)
	pTSI := pMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(pTSI)
	s.True(pTSI.GetConfig().GetEnabled())
	s.approxDuration(time.Hour, pTSI.GetConfig().GetPropagatedSkippedDuration().AsDuration(),
		"P.PropagatedSkippedDuration == G's accumulated at P-start (1h)")
	s.approxDuration(3*time.Hour, pTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"P: 2h (pt1) + 1h (pt2 after C completes) = 3h")

	// ---- Child (direct-parent-only propagation) ----
	cMS := s.getMutableStateByID(env, ctx, cWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, cMS.State.ExecutionState.State)
	cTSI := cMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(cTSI)
	s.True(cTSI.GetConfig().GetEnabled())
	s.approxDuration(2*time.Hour, cTSI.GetConfig().GetPropagatedSkippedDuration().AsDuration(),
		"C.PropagatedSkippedDuration == P's accumulated at C-start (2h); grandparent skip is NOT re-added")
	s.approxDuration(time.Hour, cTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"C accumulated 1h from ct")

	// History-side check: G's initiated event for P snapshots PSD=1h (G's accumulated at
	// command time); P's initiated event for C snapshots PSD=2h (P's accumulated at its
	// own command time — grandparent contribution is NOT carried transitively).
	gInit := s.initiatedChildEvents(ctx, env, gWFID, gRunID)
	s.Len(gInit, 1)
	gInitTSC := gInit[0].GetStartChildWorkflowExecutionInitiatedEventAttributes().GetTimeSkippingConfig()
	s.NotNil(gInitTSC)
	s.approxDuration(time.Hour, gInitTSC.GetPropagatedSkippedDuration().AsDuration())

	pRunID := pMS.State.ExecutionState.RunId
	pInit := s.initiatedChildEvents(ctx, env, pWFID, pRunID)
	s.Len(pInit, 1)
	pInitTSC := pInit[0].GetStartChildWorkflowExecutionInitiatedEventAttributes().GetTimeSkippingConfig()
	s.NotNil(pInitTSC)
	s.approxDuration(2*time.Hour, pInitTSC.GetPropagatedSkippedDuration().AsDuration())
}

// approxDuration asserts actual is within 5s of expected. Each skip loses
// (event_time - command_apply_time) of wall clock because
// AccumulatedSkippedDuration is computed as TargetTime - event.EventTime in
// ApplyWorkflowExecutionTimeSkippingTransitionedEvent; that drift accrues per
// skip and across nested workflows.
func (s *TimeSkippingPropagationTestSuite) approxDuration(expected, actual time.Duration, msgAndArgs ...any) {
	s.InDelta(float64(expected), float64(actual), float64(5*time.Second), msgAndArgs...)
}

// initiatedChildEvents returns every StartChildWorkflowExecutionInitiated event in the
// given parent's history in order. Used to assert the TimeSkippingConfig snapshot that
// the transfer task will consume when the child is actually started.
func (s *TimeSkippingPropagationTestSuite) initiatedChildEvents(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID string,
) []*historypb.HistoryEvent {
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.NoError(err)
	var out []*historypb.HistoryEvent
	for _, e := range histResp.History.Events {
		if e.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			out = append(out, e)
		}
	}
	return out
}

// drivePollsUntilClosed runs the task poller up to maxIterations times or until the
// named workflow reaches COMPLETED. Poll errors (typically long-poll timeouts) are
// logged and ignored so transient emptiness doesn't fail the test.
func (s *TimeSkippingPropagationTestSuite) drivePollsUntilClosed(
	env *testcore.TestEnv,
	ctx context.Context,
	tv *testvars.TestVars,
	handler func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	workflowID, runID string,
	maxIterations int,
) {
	for i := range maxIterations {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
		if pollErr != nil {
			s.T().Logf("iter %d: poll error (likely timeout): %v", i, pollErr)
		}
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		s.NoError(err)
		if desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return
		}
	}
}

// getMutableState loads the persistence-layer mutable state for a workflow run.
// Mirrors the helper in timeskipping_test.go to keep this file self-contained.
func (s *TimeSkippingPropagationTestSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		workflowID,
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(testcore.NewContext(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}

// getMutableStateByID resolves the current run ID of a workflow via DescribeWorkflowExecution
// and returns its persistence-layer mutable state.
func (s *TimeSkippingPropagationTestSuite) getMutableStateByID(env *testcore.TestEnv, ctx context.Context, workflowID string) *persistence.GetWorkflowExecutionResponse {
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.NoError(err)
	return s.getMutableState(env, workflowID, desc.WorkflowExecutionInfo.Execution.RunId)
}

// wftHandler is the signature of a per-workflow-type handler used by typeDispatch.
type wftHandler = func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error)

// typeDispatch routes a polled workflow task to a handler keyed by the workflow type name.
// Returns an error for unexpected types so a test failure pinpoints a mis-routed task.
func typeDispatch(handlers map[string]wftHandler) wftHandler {
	return func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		name := task.WorkflowType.GetName()
		h, ok := handlers[name]
		if !ok {
			return nil, fmt.Errorf("unexpected workflow type: %q", name)
		}
		return h(task)
	}
}

// firedTimers returns the set of TimerFired event timer IDs delivered in the WFT batch
// (events newer than task.PreviousStartedEventId).
func firedTimers(task *workflowservice.PollWorkflowTaskQueueResponse) map[string]bool {
	out := map[string]bool{}
	events := task.History.Events
	from := int(task.PreviousStartedEventId)
	if from < 0 || from > len(events) {
		from = 0
	}
	for _, e := range events[from:] {
		if e.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED {
			out[e.GetTimerFiredEventAttributes().GetTimerId()] = true
		}
	}
	return out
}

// timerCmd builds a StartTimerCommand with the given id and StartToFireTimeout.
func timerCmd(id string, d time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            id,
				StartToFireTimeout: durationpb.New(d),
			},
		},
	}
}

// childCmd builds a StartChildWorkflowExecutionCommand with ABANDON parent-close policy
// and generous (virtual-time) run/task timeouts appropriate for time-skipping tests.
func childCmd(ns, wfID string, wfType *commonpb.WorkflowType, tq *taskqueuepb.TaskQueue) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				Namespace:           ns,
				WorkflowId:          wfID,
				WorkflowType:        wfType,
				TaskQueue:           tq,
				WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_ABANDON,
			},
		},
	}
}

// completeCmd builds a CompleteWorkflowExecution command with an empty payload.
func completeCmd() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

// cmdsResponse wraps a list of commands in a RespondWorkflowTaskCompletedRequest.
func cmdsResponse(cmds ...*commandpb.Command) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: cmds}
}

// firstWorkflowTaskCompletedEventID returns the event ID of the first
// WorkflowTaskCompleted event in the given run's history. Used as the reset
// point for tests that need to rewind to just past the first WFT.
func (s *TimeSkippingPropagationTestSuite) firstWorkflowTaskCompletedEventID(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID string,
) int64 {
	hist, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.NoError(err)
	for _, e := range hist.History.Events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return e.GetEventId()
		}
	}
	s.FailNow("no WorkflowTaskCompleted event found in history")
	return 0
}

// TestReset_OptionsUpdateIsReapplied_TimeSkippingEnabled documents the
// interaction between reset and WorkflowExecutionOptionsUpdated events.
//
// Scenario:
//   - Start W with TimeSkippingConfig{Enabled: false}.
//   - Drive WFT 1 with no commands → workflow idles after first WFTCompleted.
//   - Call UpdateWorkflowExecutionOptions setting Enabled: true → emits
//     OPTIONS_UPDATED event (does not schedule a WFT).
//   - Terminate W so it closes cleanly before reset.
//   - Reset to the first WFTCompleted event, with default reapply (no excludes).
//   - Drive the reset run's fresh WFT to completion.
//
// Expected: the reset run has TimeSkippingConfig.Enabled == true even though
// the reset point precedes the options update. This is because OPTIONS_UPDATED
// is reapplied unconditionally at workflow_resetter.go — there is no
// ResetReapplyExcludeType value that can suppress it. The test pins this
// behavior so a future change would surface it.
func (s *TimeSkippingPropagationTestSuite) TestPropagationInReset() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	wfID := tv.WorkflowID()

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: false},
	})
	s.NoError(err)
	originalRunID := startResp.RunId

	// Drive WFT 1 with an empty command response so the workflow completes its
	// first task and idles. This gives us a stable reset point before the
	// options update.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err)

	resetEventID := s.firstWorkflowTaskCompletedEventID(ctx, env, wfID, originalRunID)

	// UpdateWorkflowExecutionOptions to enable time-skipping. This emits a
	// WorkflowExecutionOptionsUpdated event. It does NOT schedule a new WFT,
	// so we terminate the workflow below to close it before resetting.
	_, err = env.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: originalRunID},
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: true},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
	})
	s.NoError(err)

	// Terminate the original workflow so it's closed; the terminate event
	// lands in history after the OPTIONS_UPDATED event. Terminate events are
	// always excluded from reapply, so the reset scan will see OPTIONS_UPDATED
	// as the only reapplyable post-reset event.
	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: originalRunID},
		Reason:            "test: close original before reset",
	})
	s.NoError(err)

	// Sanity: original run ended with TSC enabled.
	origMS := s.getMutableState(env, wfID, originalRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, origMS.State.ExecutionState.State)
	s.True(origMS.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled(),
		"original run should have TSC enabled after UpdateWorkflowExecutionOptions")

	// Reset to the first WFTCompleted event with default reapply.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: originalRunID},
		Reason:                    "test: verify OPTIONS_UPDATED reapply preserves TimeSkippingConfig",
		WorkflowTaskFinishEventId: resetEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRunID := resetResp.RunId
	s.NotEqual(originalRunID, resetRunID, "reset must produce a fresh run")

	// Drive the reset run's fresh WFT to completion.
	s.drivePollsUntilClosed(env, ctx, tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return cmdsResponse(completeCmd()), nil
	}, wfID, resetRunID, 10)

	// Key assertion: the reset run has TSC enabled because the post-reset
	// OPTIONS_UPDATED event was reapplied onto the new run.
	resetMS := s.getMutableState(env, wfID, resetRunID)
	s.True(resetMS.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled(),
		"reset run should have TSC enabled; OPTIONS_UPDATED event is reapplied from original run")

	// Confirm the mechanism: the reset run's history contains exactly one
	// reapplied OPTIONS_UPDATED event, and the WorkflowExecutionStarted event
	// still reflects the original disabled config (i.e. replay restored the
	// start-point state before reapply enabled it).
	resetHist, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: resetRunID},
	})
	s.NoError(err)
	var optionsUpdatedCount int
	var startedEvent *historypb.HistoryEvent
	for _, e := range resetHist.History.Events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedCount++
		}
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			startedEvent = e
		}
	}
	s.Equal(1, optionsUpdatedCount,
		"reset run should contain exactly one reapplied OPTIONS_UPDATED event")
	s.NotNil(startedEvent)
	startedTSC := startedEvent.GetWorkflowExecutionStartedEventAttributes().GetTimeSkippingConfig()
	s.NotNil(startedTSC, "WorkflowExecutionStarted should carry the original TimeSkippingConfig")
	s.False(startedTSC.GetEnabled(),
		"WorkflowExecutionStarted on reset run should carry the original disabled config; the later enable comes from reapply, not replay")
}
