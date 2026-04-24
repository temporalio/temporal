// Tests in this file exercise how TimeSkippingConfig and AccumulatedSkippedDuration
// cross workflow boundaries. Each test pins one of three inheritance semantics.
//
// Group 1 — Inherit both from the current execution.
//
//	Continue-as-new: a new run is a technical continuation of the same logical run,
//	so the current config AND accumulated skipped duration are both inherited. The
//	configured bound is shared: it caps the sum of inherited skip + new-run skip,
//	not each separately.
//
//	Child workflows: same as continue-as-new. A child can be viewed as either an
//	extension of the parent or a separate workflow; either way, virtual time must
//	not rewind, so the child inherits the parent's accumulated skip. The default
//	behavior treats the child as an extension and shares the config; a child-level
//	override is only added when explicitly needed.
//
//	Covered by:
//	  - TestTSPInChildWf_Basic
//	  - TestTSPInChildWf_TwoChildren
//	  - TestTSPInChildWf_ThreeGenerations
//	  - TestTSPInCaN
//
// Group 2 — Inherit from a specific point in history.
//
//	Retry: a retried run is defined as restarting execution from the previous run's
//	WorkflowExecutionStarted event. The new run inherits the config and PSD recorded
//	on that event verbatim; in-flight skip accumulated during the failed attempt is
//	NOT carried forward.
//
//	Cron: same as retry. Each cron run restarts from the previous run's event #1
//	snapshot; the previous run's in-flight skip is not carried forward.
//
//	Covered by:
//	  - TestTSPInRetry
//	  - TestTSPInCron
//
// Group 3 — Inherit from a specific point, then catch up on config changes.
//
//	Reset: retains the current TimeSkippingConfig at reset time. Reset replays all
//	events up to the reset point (giving the new run the config as-of that point),
//	then reapplies any WorkflowExecutionOptionsUpdated events that occurred between
//	the reset point and the original run's terminal state. Reapply is unconditional
//	— there is no ResetReapplyExcludeType value that can suppress OPTIONS_UPDATED.
//	AccumulatedSkippedDuration on the reset run starts from the event #1
//	InitialSkippedDuration (same as Group 2); original-run in-flight skip is not
//	carried forward.
//
//	Covered by:
//	  - TestTSPInReset
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
	failurepb "go.temporal.io/api/failure/v1"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type TimeSkippingPropagationTestSuite struct {
	parallelsuite.Suite[*TimeSkippingPropagationTestSuite]
}

func TestTimeSkippingPropagationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingPropagationTestSuite{})
}

// TestTSPInChildWf_Basic exercises end-to-end
// propagation of TimeSkippingConfig + AccumulatedSkippedDuration from parent to child,
// together with the idle-only skipping invariant on the parent.
//
// The parent's StartChildWorkflowExecutionInitiated event carries an InitialSkippedDuration
// snapshot of the parent's accumulated skip at child-start time (separate from the
// TimeSkippingConfig Config snapshot). applyTimeSkippingConfig on the child's MS uses the
// initial skip to seed AccumulatedSkippedDuration, so the child's virtual clock continues
// from where the parent left off.
//
// Scenario:
//   - Parent starts with TimeSkippingConfig{Enabled: true}.
//   - Parent issues StartTimer(t1, 1h). Parent is idle on t1 → server skips 1h.
//     After skip: parent.AccumulatedSkippedDuration = 1h.
//   - t1 fires. Parent issues StartChildWorkflow(child) and StartTimer(t2, 1h).
//     Parent now has a pending child → NOT idle → parent does NOT skip.
//   - Child is started. Its MS is seeded with AccumulatedSkippedDuration = 1h
//     (from the InitialSkippedDuration=1h on the initiated event) and Config.Enabled
//     = true (cloned verbatim from parent's Config).
//   - Child issues StartTimer(tc, 3h). Child is idle → server skips 3h.
//     After skip: child.AccumulatedSkippedDuration = 1h + 3h = 4h.
//   - tc fires. Child completes.
//   - Parent receives ChildWorkflowExecutionCompleted. Parent is now only waiting
//     on t2 → idle → server skips 1h to fire t2.
//     After skip: parent.AccumulatedSkippedDuration = 2h.
//   - t2 fires. Parent completes.
//
// End-state assertions:
//   - parent.AccumulatedSkippedDuration == 2h.
//   - child.TimeSkippingInfo.Config.Enabled == true (propagated).
//   - child.AccumulatedSkippedDuration == 4h (1h inherited + 3h own from tc).
//   - The parent's StartChildWorkflowExecutionInitiated event carries
//     TimeSkippingConfig{Enabled=true} and InitialSkippedDuration=1h (the pre-apply
//     snapshots; the wire form the transfer task consumes).
//   - Wall-clock elapsed is nowhere near 5h of virtual time.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_Basic() {
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

	s.drivePollsUntilClosed(ctx, env, tv, dispatch, parentWFID, parentRunID, 20)

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
	childMS := s.getMutableStateByID(ctx, env, childWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State)
	childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(childTSI)
	childCfg := childTSI.GetConfig()
	s.NotNil(childCfg)
	s.True(childCfg.GetEnabled(), "child TSC.Enabled propagated from parent")
	s.approxDuration(4*time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"child AccumulatedSkippedDuration == parent's accumulated at child-start (1h) + child's own skip for tc (3h) = 4h")

	// The parent's StartChildWorkflowExecutionInitiated event must carry the propagated
	// TimeSkippingConfig and InitialSkippedDuration — these are what the transfer task
	// consumes when it starts the child.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 1)
	initAttrs := initEvents[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	initTSC := initAttrs.GetTimeSkippingConfig()
	s.NotNil(initTSC, "initiated event should carry TimeSkippingConfig snapshot")
	s.True(initTSC.GetEnabled(), "snapshot mirrors parent's Enabled flag")
	s.approxDuration(time.Hour, initAttrs.GetInitialSkippedDuration().AsDuration(),
		"initiated event InitialSkippedDuration == parent's AccumulatedSkippedDuration at command time (1h)")
}

// TestTSPInChildWf_TwoChildren verifies that:
//   - All children started in the same WFT inherit the parent's TimeSkippingConfig
//     and have their AccumulatedSkippedDuration seeded to the parent's accumulated
//     skip at child-start time (via InitialSkippedDuration on the initiated event,
//     which applyTimeSkippingConfig converts into AccumulatedSkippedDuration).
//   - The parent is blocked from skipping while ANY child is pending, and resumes
//     skipping only after all children complete (idle-only invariant).
//
// Scenario:
//   - Parent TSC enabled. Parent issues StartTimer(t1, 1h) → skips 1h.
//   - Parent issues [StartChild(c1), StartChild(c2), StartTimer(t2, 1h)].
//     Each child's MS is seeded with AccumulatedSkippedDuration = 1h
//     (inherited from the parent's accumulated at command time).
//   - c1 issues StartTimer(tc1, 2h) → idle → skips 2h → completes.
//   - c2 issues StartTimer(tc2, 2h) → idle → skips 2h → completes.
//   - Only after BOTH c1 and c2 complete does parent become idle on t2 → skips 1h → t2 fires.
//   - Parent completes.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_TwoChildren() {
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

	s.drivePollsUntilClosed(ctx, env, tv, dispatch, parentWFID, parentRunID, 30)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than virtual time", elapsed)

	// ---- Parent ----
	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, parentMS.State.ExecutionState.State)
	s.approxDuration(2*time.Hour, parentMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration(),
		"parent skip: 1h (t1) + 1h (t2 after BOTH children complete) = 2h")

	// ---- Each child inherits identically ----
	for _, id := range []string{child1WFID, child2WFID} {
		childMS := s.getMutableStateByID(ctx, env, id)
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State, "%s", id)
		childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
		s.NotNil(childTSI, "%s TSI", id)
		s.True(childTSI.GetConfig().GetEnabled(), "%s inherited Enabled", id)
		s.approxDuration(3*time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
			"%s AccumulatedSkippedDuration == parent's accumulated at child-start (1h) + own skip for tc (2h) = 3h", id)
	}

	// Both children are initiated in the same WFT — the parent's history must carry two
	// StartChildWorkflowExecutionInitiated events, each with the snapshot (Config{Enabled=true},
	// InitialSkippedDuration=1h) computed from parent state at command time.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 2)
	for _, e := range initEvents {
		attrs := e.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		tsc := attrs.GetTimeSkippingConfig()
		s.NotNil(tsc)
		s.True(tsc.GetEnabled())
		s.approxDuration(time.Hour, attrs.GetInitialSkippedDuration().AsDuration())
	}
}

// TestTSPInChildWf_ThreeGenerations verifies that AccumulatedSkippedDuration
// flows transitively through the ancestry: each generation's MS starts with its direct
// parent's accumulated skip (which already includes all ancestors' contributions) seeded
// into AccumulatedSkippedDuration, then adds its own skipping on top.
//
// The initiated-event InitialSkippedDuration snapshot on any parent reflects that parent's
// full accumulated skip at command time — which includes the parent's own inherited portion.
// There is no "direct-parent-only" carve-out; the ancestry's virtual clock is continuous.
//
// Scenario:
//   - Grandparent (G) TSC enabled. G: StartTimer(gt1, 1h) → skips 1h → G.accum = 1h.
//   - G: StartChild(P) + StartTimer(gt2, 1h). P's initiated event snapshots
//     InitialSkippedDuration=1h. P's MS: AccumulatedSkippedDuration seeded to 1h.
//   - P: StartTimer(pt1, 2h) → skips 2h → P.accum = 1h + 2h = 3h.
//   - P: StartChild(C) + StartTimer(pt2, 1h). C's initiated event snapshots
//     InitialSkippedDuration=3h (P's full accumulated at command time, which already
//     includes G's 1h). C's MS: AccumulatedSkippedDuration seeded to 3h.
//   - C: StartTimer(ct, 1h) → skips 1h → C.accum = 3h + 1h = 4h → completes.
//   - P: idle on pt2 → skips 1h → P.accum = 3h + 1h = 4h. pt2 fires. P completes.
//   - G: idle on gt2 → skips 1h → G.accum = 1h + 1h = 2h. gt2 fires. G completes.
//
// Final state: C.accum == 4h (3h inherited + 1h own); P.accum == 4h; G.accum == 2h.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ThreeGenerations() {
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

	s.drivePollsUntilClosed(ctx, env, tv, dispatch, gWFID, gRunID, 40)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 3*time.Minute, "wall-clock elapsed (%s) should be far less than virtual time", elapsed)

	// ---- Grandparent ----
	gMS := s.getMutableState(env, gWFID, gRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, gMS.State.ExecutionState.State)
	s.approxDuration(2*time.Hour, gMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration(),
		"G: 1h (gt1) + 1h (gt2 after P completes) = 2h")

	// ---- Parent ----
	pMS := s.getMutableStateByID(ctx, env, pWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, pMS.State.ExecutionState.State)
	pTSI := pMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(pTSI)
	s.True(pTSI.GetConfig().GetEnabled())
	s.approxDuration(4*time.Hour, pTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"P.accum == G's accumulated at P-start (1h) + P's own pt1 (2h) + P's own pt2 (1h) = 4h")

	// ---- Child (accum inherited transitively through the ancestry) ----
	cMS := s.getMutableStateByID(ctx, env, cWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, cMS.State.ExecutionState.State)
	cTSI := cMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(cTSI)
	s.True(cTSI.GetConfig().GetEnabled())
	s.approxDuration(4*time.Hour, cTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"C.accum == P's accumulated at C-start (3h, which already includes G's 1h) + C's own ct (1h) = 4h")

	// History-side check: G's initiated event for P snapshots InitialSkippedDuration=1h
	// (G's accumulated at command time); P's initiated event for C snapshots
	// InitialSkippedDuration=3h (P's full accumulated at command time — includes G's 1h
	// contribution, which propagates transitively).
	gInit := s.initiatedChildEvents(ctx, env, gWFID, gRunID)
	s.Len(gInit, 1)
	gInitAttrs := gInit[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	s.NotNil(gInitAttrs.GetTimeSkippingConfig())
	s.approxDuration(time.Hour, gInitAttrs.GetInitialSkippedDuration().AsDuration())

	pRunID := pMS.State.ExecutionState.RunId
	pInit := s.initiatedChildEvents(ctx, env, pWFID, pRunID)
	s.Len(pInit, 1)
	pInitAttrs := pInit[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	s.NotNil(pInitAttrs.GetTimeSkippingConfig())
	s.approxDuration(3*time.Hour, pInitAttrs.GetInitialSkippedDuration().AsDuration(),
		"P's initiated event for C carries P's full accumulated at command time (3h = G's 1h + P's pt1 2h)")
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
	ctx context.Context,
	env *testcore.TestEnv,
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

// drivePollsUntilRunNotRunning is like drivePollsUntilClosed but exits on any non-RUNNING
// status. Used for cron, where a run rolls to CONTINUED_AS_NEW rather than COMPLETED and
// the chain's "current run" keeps advancing indefinitely.
func (s *TimeSkippingPropagationTestSuite) drivePollsUntilRunNotRunning(
	ctx context.Context,
	env *testcore.TestEnv,
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
		if desc.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
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
func (s *TimeSkippingPropagationTestSuite) getMutableStateByID(ctx context.Context, env *testcore.TestEnv, workflowID string) *persistence.GetWorkflowExecutionResponse {
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

// failCmd builds a FailWorkflowExecution command with a bare Failure{Message: msg}.
// Such a failure carries no specific FailureInfo, so retry.isRetryable treats it as
// retryable — the server schedules a new run under RETRY initiator when RetryPolicy allows.
func failCmd(msg string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
			FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: &failurepb.Failure{Message: msg},
			},
		},
	}
}

// continueAsNewCmd builds a ContinueAsNewWorkflowExecution command with the given workflow
// type and task queue, using the same virtual-time timeouts as the other command helpers.
func continueAsNewCmd(wfType *commonpb.WorkflowType, tq *taskqueuepb.TaskQueue) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType:        wfType,
				TaskQueue:           tq,
				WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
				WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			},
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
func (s *TimeSkippingPropagationTestSuite) TestTSPInReset() {
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
	s.drivePollsUntilClosed(ctx, env, tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
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

// TestTSPInCaN verifies that TimeSkippingConfig and the previous run's
// AccumulatedSkippedDuration are propagated to the new run on continue-as-new.
//
// CaN uses the same propagation mechanism as child workflows: the previous run's
// AccumulatedSkippedDuration is snapshotted as InitialSkippedDuration on the new
// run's WorkflowExecutionStarted event (separate from the TimeSkippingConfig snapshot).
// applyTimeSkippingConfig uses InitialSkippedDuration to seed AccumulatedSkippedDuration
// on the new MS, so the virtual clock continues seamlessly.
//
// Scenario:
//   - Run 1 starts with TimeSkippingConfig{Enabled: true}.
//   - Run 1 issues StartTimer(t1, 1h). Idle → server skips 1h.
//     After skip: run1.AccumulatedSkippedDuration = 1h.
//   - t1 fires. Run 1 issues ContinueAsNew (same type, same task queue).
//   - Run 2 starts. Its WorkflowExecutionStarted event carries
//     TimeSkippingConfig{Enabled=true} and InitialSkippedDuration=1h. The applied MS
//     has Config.Enabled=true and AccumulatedSkippedDuration=1h.
//   - Run 2 issues StartTimer(t2, 2h). Idle → skips 2h.
//     After skip: run2.AccumulatedSkippedDuration = 1h + 2h = 3h.
//   - t2 fires. Run 2 completes.
//
// End-state assertions:
//   - run1.status == CONTINUED_AS_NEW, run1.AccumulatedSkippedDuration == 1h.
//   - run2.status == COMPLETED.
//   - run2.TimeSkippingInfo.Config.Enabled == true (propagated).
//   - run2.AccumulatedSkippedDuration == 3h.
//   - run2 WorkflowExecutionStarted event retains the InitialSkippedDuration=1h snapshot
//     (history observability — the event is created before applyTimeSkippingConfig runs).
//   - Wall-clock elapsed is nowhere near 3h of virtual time.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCaN() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	startWall := time.Now()

	start1, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        wfType,
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	run1ID := start1.RunId

	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case state == 0:
			state = 1
			return cmdsResponse(timerCmd("t1", time.Hour)), nil
		case state == 1 && fired["t1"]:
			state = 2
			return cmdsResponse(continueAsNewCmd(wfType, tq)), nil
		case state == 2:
			state = 3
			return cmdsResponse(timerCmd("t2", 2*time.Hour)), nil
		case state == 3 && fired["t2"]:
			state = 4
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	// Empty runID so describe follows the chain to the current run (run 2 after CaN).
	s.drivePollsUntilClosed(ctx, env, tv, handler, wfID, "", 20)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 3h of virtual time", elapsed)

	// ---- Run 1: closed with CONTINUED_AS_NEW ----
	run1MS := s.getMutableState(env, wfID, run1ID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, run1MS.State.ExecutionState.Status)
	run1TSI := run1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run1TSI)
	s.approxDuration(time.Hour, run1TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"run 1 should have skipped 1h for t1 before continue-as-new")

	// ---- Run 2: the CaN'd-into run, completed ----
	run2MS := s.getMutableStateByID(ctx, env, wfID)
	run2ID := run2MS.State.ExecutionState.RunId
	s.NotEqual(run1ID, run2ID, "run 2 should have a new run ID")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, run2MS.State.ExecutionState.Status)
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI, "run 2 should have TimeSkippingInfo propagated from run 1")
	run2Cfg := run2TSI.GetConfig()
	s.NotNil(run2Cfg)
	s.True(run2Cfg.GetEnabled(), "run 2 TSC.Enabled propagated from run 1")
	s.approxDuration(3*time.Hour, run2TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"run 2 AccumulatedSkippedDuration == run 1's accumulated (1h) + run 2's own skip for t2 (2h) = 3h")

	// Run 2's WorkflowExecutionStarted event must carry the propagated TSC snapshot.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	s.Equal(run1ID, startedAttr.GetContinuedExecutionRunId(),
		"run 2 should reference run 1 as its predecessor")
	startedTSC := startedAttr.GetTimeSkippingConfig()
	s.NotNil(startedTSC, "run 2's WorkflowExecutionStarted must carry the TSC snapshot")
	s.True(startedTSC.GetEnabled(), "started event TSC.Enabled mirrors run 1's config")
	s.approxDuration(time.Hour, startedAttr.GetInitialSkippedDuration().AsDuration(),
		"started event retains InitialSkippedDuration=1h (run 1's accumulated at CaN time); applyTimeSkippingConfig uses it to seed AccumulatedSkippedDuration on the MS but the event snapshot is unchanged")
}

// TestTSPInRetry verifies that TimeSkippingConfig is propagated from the
// original run's WorkflowExecutionStarted event to a retried workflow run.
//
// Retry's createRequest.TimeSkippingConfig is a verbatim copy of startAttr.TimeSkippingConfig
// (event #1 of the previous run), and req.InitialSkippedDuration is a verbatim copy of
// startAttr.InitialSkippedDuration. applyTimeSkippingConfig then produces the same MS state
// the original run was booted with: Config.Enabled preserved; if the original event #1 had
// a non-zero InitialSkippedDuration snapshot (e.g., the original run was itself started as
// a child or via CaN), that value seeds AccumulatedSkippedDuration on the retry — otherwise
// the retry starts with Accum=0.
//
// Key semantic: unlike continue-as-new, the previous attempt's *in-flight* accumulated
// skip (skip that happened during the attempt) is NOT carried forward to the retry.
// Each attempt gets a fresh virtual-time clock at whatever offset the original run was
// booted with — the retry is semantically a re-attempt, not a continuation.
//
// Scenario:
//   - Start a top-level workflow with TimeSkippingConfig{Enabled: true} and
//     RetryPolicy{MaximumAttempts: 2}. Event #1's InitialSkippedDuration is unset
//     (no parent, no prior CaN).
//   - Attempt 1 issues StartTimer(t1, 1h). Idle → server skips 1h → attempt1.Accum = 1h.
//   - Attempt 1 issues FailWorkflowExecution with a bare (retryable) Failure.
//   - Server schedules retry. Attempt 2 (retry run) starts.
//     Its MS applies startAttr.TimeSkippingConfig: Config.Enabled=true, no initial skip
//     → Accum=0. Attempt 1's 1h in-flight skip is NOT carried forward.
//   - Attempt 2 issues StartTimer(t2, 2h). Idle → server skips 2h → attempt2.Accum = 2h.
//   - Attempt 2 completes.
//
// End-state assertions:
//   - Attempt 1: status=FAILED, Config.Enabled=true, Accum=1h.
//   - Attempt 2: status=COMPLETED, Config.Enabled=true, Accum=2h (own skip only;
//     1h from attempt 1 is NOT inherited).
//   - Attempt 2's WorkflowExecutionStarted carries the same TSC snapshot as attempt 1's
//     event #1 (Enabled=true, no InitialSkippedDuration) and references attempt 1 as
//     ContinuedExecutionRunId.
func (s *TimeSkippingPropagationTestSuite) TestTSPInRetry() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	startWall := time.Now()

	start1, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        wfType,
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(time.Second),
			BackoffCoefficient: 1.0,
			MaximumAttempts:    2,
		},
	})
	s.NoError(err)
	attempt1ID := start1.RunId

	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case state == 0:
			state = 1
			return cmdsResponse(timerCmd("t1", time.Hour)), nil
		case state == 1 && fired["t1"]:
			state = 2
			return cmdsResponse(failCmd("retry me")), nil
		case state == 2:
			state = 3
			return cmdsResponse(timerCmd("t2", 2*time.Hour)), nil
		case state == 3 && fired["t2"]:
			state = 4
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	// Empty runID so describe follows the chain to the current run (attempt 2 after retry).
	s.drivePollsUntilClosed(ctx, env, tv, handler, wfID, "", 20)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 3h of virtual time", elapsed)

	// ---- Attempt 1: failed, retried ----
	attempt1MS := s.getMutableState(env, wfID, attempt1ID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, attempt1MS.State.ExecutionState.Status)
	attempt1TSI := attempt1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(attempt1TSI)
	s.True(attempt1TSI.GetConfig().GetEnabled(), "attempt 1 has TSC enabled from the initial start request")
	s.approxDuration(time.Hour, attempt1TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"attempt 1 skipped 1h for t1 before failing")

	// ---- Attempt 2: retry run, completed ----
	attempt2MS := s.getMutableStateByID(ctx, env, wfID)
	attempt2ID := attempt2MS.State.ExecutionState.RunId
	s.NotEqual(attempt1ID, attempt2ID, "attempt 2 has a new run ID")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, attempt2MS.State.ExecutionState.Status)
	attempt2TSI := attempt2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(attempt2TSI, "attempt 2 should have TimeSkippingInfo from the propagated event #1 snapshot")
	attempt2Cfg := attempt2TSI.GetConfig()
	s.NotNil(attempt2Cfg)
	s.True(attempt2Cfg.GetEnabled(), "attempt 2 TSC.Enabled propagated from attempt 1's event #1")
	s.approxDuration(2*time.Hour, attempt2TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"attempt 2 accumulated only its own 2h from t2; attempt 1's 1h in-flight skip is NOT carried forward on retry")

	// Attempt 2's WorkflowExecutionStarted must carry the same TSC snapshot as attempt 1's event #1.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: attempt2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	s.Equal(attempt1ID, startedAttr.GetContinuedExecutionRunId(),
		"attempt 2 references attempt 1 as its predecessor via ContinuedExecutionRunId")
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY, startedAttr.GetInitiator(),
		"attempt 2 is initiated by retry")
	startedTSC := startedAttr.GetTimeSkippingConfig()
	s.NotNil(startedTSC, "attempt 2's WorkflowExecutionStarted must carry the TSC snapshot")
	s.True(startedTSC.GetEnabled(), "started event TSC.Enabled mirrors attempt 1's event #1")
	s.Zero(startedAttr.GetInitialSkippedDuration().AsDuration(),
		"started event InitialSkippedDuration matches attempt 1's event #1 (unset for a top-level workflow)")
}

// TestTSPInCron verifies that TimeSkippingConfig (Enabled + Bound) is
// propagated from the previous run's WorkflowExecutionStarted event to a cron-scheduled
// next run verbatim, while AccumulatedSkippedDuration is NOT inherited.
//
// Cron and retry share SetupNewWorkflowForRetryOrCron, which copies
// createRequest.TimeSkippingConfig = startAttr.GetTimeSkippingConfig() and
// req.InitialSkippedDuration = startAttr.GetInitialSkippedDuration() from event #1 of
// the previous run onto the new run. applyTimeSkippingConfig then seeds
// AccumulatedSkippedDuration from InitialSkippedDuration. For a top-level cron workflow,
// event #1 has no InitialSkippedDuration, so each cron run starts with Accum=0 — the
// previous run's in-flight skip is NOT carried forward.
//
// Scenario:
//   - Start a cron workflow (`* * * * *`) with TimeSkippingConfig{Enabled: true,
//     Bound: MaxSkippedDuration(1h)}. Event #1's InitialSkippedDuration is unset
//     (no parent, no prior CaN).
//   - Run 1 issues StartTimer(t1, 50min). Idle → server skips 50min → run1.Accum ≈ 50min.
//   - Run 1 completes. Cron rolls forward; server creates run 2 with
//     createRequest.TimeSkippingConfig = run 1's event #1 TSC (Enabled=true,
//     Bound=MaxSkippedDuration(1h)) and no InitialSkippedDuration → run 2 MS has
//     Config{Enabled: true, Bound: MaxSkippedDuration(1h)}, Accum=0.
//   - Run 2 completes immediately (no own skipping). Run 1's 50min accum is NOT inherited.
//
// End-state assertions:
//   - Run 1: Config{Enabled, Bound=1h}, Accum ≈ 50min.
//   - Run 2: Config{Enabled, Bound=1h} (inherited verbatim),
//     Accum ≪ 50min (the previous run's in-flight skip is not carried forward).
//   - Run 2's WorkflowExecutionStarted event carries the verbatim TSC snapshot from
//     run 1's event #1, with initiator=CRON_SCHEDULE.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCron() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	bound := &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
		MaxSkippedDuration: durationpb.New(time.Hour),
	}
	inputCfg := &workflowpb.TimeSkippingConfig{Enabled: true, Bound: bound}

	startWall := time.Now()

	start1, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        wfType,
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  inputCfg,
		CronSchedule:        "* * * * *",
	})
	s.NoError(err)
	run1ID := start1.RunId

	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case state == 0:
			state = 1
			return cmdsResponse(timerCmd("t1", 50*time.Minute)), nil
		case state == 1 && fired["t1"]:
			state = 2
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	// Drive run 1 to non-RUNNING (CONTINUED_AS_NEW once cron rolls forward). Run 2's
	// assertions below only read state that's populated at run 2 creation (mutable state
	// + event #1), so we don't need to drive run 2 through its cron backoff.
	s.drivePollsUntilRunNotRunning(ctx, env, tv, handler, wfID, run1ID, 30)

	// After run 1 closes, describe (no runID) gives the current chain run — that's run 2.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
	})
	s.NoError(err)
	run2ID := desc.WorkflowExecutionInfo.Execution.RunId
	s.NotEqual(run1ID, run2ID, "cron should have rolled forward to a new run")

	elapsed := time.Since(startWall)
	s.Less(elapsed, 3*time.Minute, "wall-clock elapsed (%s) should be far less than virtual time", elapsed)

	// Stop the cron chain so run 2 (still in cron backoff) and any subsequent runs don't fire.
	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID},
		Reason:            "test cleanup: stop cron chain",
	})

	// ---- Run 1: Config{Enabled, Bound=1h}, Accum ≈ 50min ----
	run1MS := s.getMutableState(env, wfID, run1ID)
	run1TSI := run1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run1TSI)
	s.True(proto.Equal(inputCfg, run1TSI.GetConfig()),
		"run 1 Config mirrors the start request: Enabled=true, Bound=MaxSkippedDuration(1h)")
	// Tolerance absorbs per-skip clock drift plus up to ~60s of cron backoff that may
	// be skipped while waiting for the first WFT.
	s.InDelta(float64(50*time.Minute), float64(run1TSI.GetAccumulatedSkippedDuration().AsDuration()), float64(90*time.Second),
		"run 1 skipped ~50min for t1 before cron rolled it forward")

	// ---- Run 2: Config inherited (Enabled + Bound); Accum NOT inherited ----
	run2MS := s.getMutableState(env, wfID, run2ID)
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI, "run 2 should have TimeSkippingInfo propagated from run 1's event #1")
	s.True(proto.Equal(inputCfg, run2TSI.GetConfig()),
		"run 2 Config matches run 1's event #1 verbatim: Enabled=true and Bound=MaxSkippedDuration(1h) are inherited")
	s.Less(run2TSI.GetAccumulatedSkippedDuration().AsDuration(), 10*time.Minute,
		"run 2 Accum is fresh — nowhere near run 1's 50min; the previous run's in-flight skip is NOT carried forward on cron")

	// Run 2's WorkflowExecutionStarted carries the verbatim TSC snapshot from run 1's event #1.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	s.Equal(run1ID, startedAttr.GetContinuedExecutionRunId(),
		"run 2 references run 1 as its predecessor via ContinuedExecutionRunId")
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, startedAttr.GetInitiator(),
		"run 2 is initiated by cron schedule")
	s.True(proto.Equal(inputCfg, startedAttr.GetTimeSkippingConfig()),
		"started event carries Enabled=true, Bound=MaxSkippedDuration(1h) — the verbatim snapshot from run 1's event #1")
	s.Zero(startedAttr.GetInitialSkippedDuration().AsDuration(),
		"started event InitialSkippedDuration is unset for a top-level cron workflow")
}
