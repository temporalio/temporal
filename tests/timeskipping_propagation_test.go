// Tests in this file exercise how TimeSkippingConfig and StatePropagation
// propagate across workflow boundaries. Each test pins one of two inheritance semantics.
//
// Group 1 — Chain of runs (continue-as-new, retry, cron).
// The policy is to propagate all TimeSkippingConfig and StatePropagation across the chain.
//
//	Covered by:
//	  - TestTSPInCaN
//	  - TestTSPInRetry
//	  - TestTSPInCron
//
// Group 2 — Child workflows.
//
//  1. fast-forward is not propagated to children.
//  2. DisablePropagation disables propagation of the enabled flag.
//  3. the child inherits the parent's accumulated skipped duration in all cases.
//     Covered by:
//     - TestTSPInChildWf_Basic
//     - TestTSPInChildWf_TwoChildren
//     - TestTSPInChildWf_ThreeGenerations
//     - TestTSPInChildWf_AdmissionTimestampsShifted
//     - TestTSPInChildWf_PropagationDisabled
//     - TestTSPInChildWf_ParentRunTimeoutBeforeChild
//     - TestTSPInChildWf_ParentTimerBeforeChild
//     - TestTSPInChildWf_ChildBeforeParentTimer
//     - TestTSPInChildWf_ActivityInFlightDoesNotSkip
//     - TestTSPInChildWf_SiblingSignalDoesNotDeadlock
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
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	historytasks "go.temporal.io/server/service/history/tasks"
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
//   - Parent starts with TimeSkippingConfig{Enabled: true, FastForward: 3h}. The
//     fast-forward never cascades into children: the child's config carries Enabled
//     only, and the child's own skipping is uncapped.
//   - Parent issues StartTimer(t1, 1h). Parent is idle on t1 → server skips 1h.
//     After skip: parent.AccumulatedSkippedDuration = 1h.
//   - t1 fires. Parent issues StartChildWorkflow(child) and StartTimer(t2, 1h).
//     Parent now has a pending child → NOT idle → parent does NOT skip.
//   - Child is started. Its MS is seeded with AccumulatedSkippedDuration = 1h
//     (from the InitialSkippedDuration=1h on the initiated event) and Config.Enabled
//     = true (cloned verbatim from parent's Config).
//   - Child issues StartTimer(tc, 3h). Child is idle → server skips 3h.
//     After skip: child.AccumulatedSkippedDuration = 1h + 3h = 4h.
//   - tc fires. The child's virtual close is 3h after the parent started it.
//     The parent's t2 is due 1h after that same moment, so t2 is delivered
//     before the child completion is written. The parent completes on t2.
//     After that skip: parent.AccumulatedSkippedDuration = 2h.
//     Real time does the same: the shorter parent timer fires while the child
//     is still running. The child completion is not in the parent's history.
//
// End-state assertions:
//   - parent.AccumulatedSkippedDuration == 2h, within its own 3h FastForward budget.
//   - child.TimeSkippingInfo.Config.Enabled == true (propagated), FastForward unset.
//   - child.AccumulatedSkippedDuration == 4h (1h inherited + 3h own from tc) — exceeding
//     the parent's 3h FastForward, which is possible only because the budget did not
//     cascade into the child.
//   - The parent's StartChildWorkflowExecutionInitiated event carries
//     TimeSkippingConfig{Enabled=true} with no FastForward, and InitialSkippedDuration=1h
//     (the pre-apply snapshots; the wire form the transfer task consumes).
//   - Wall-clock elapsed is nowhere near 5h of virtual time.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_Basic() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		// FastForward never cascades into children; the parent's 2h of own skipping
		// stays under this budget so the parent's flow is unaffected by it.
		TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour), Id: "ff-id"},
		},
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
	s.Equal(3*time.Hour, parentTSI.GetConfig().GetFastForwardConfig().GetDuration().AsDuration(),
		"parent keeps its own MaxElapsedDuration; child propagation must not mutate it")
	s.approxDuration(2*time.Hour, parentTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"parent should accumulate 1h (t1) + 1h (t2, due before the child finishes) = 2h")
	s.True(s.timerFiredBeforeChildCompletion(ctx, env, parentWFID, parentRunID, "t2"),
		"t2 fires before the child completion is recorded; the parent closes on t2")

	// ---- Child ----
	childMS := s.getMutableStateByID(ctx, env, childWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State)
	childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(childTSI)
	childCfg := childTSI.GetConfig()
	s.NotNil(childCfg)
	s.True(childCfg.GetEnabled(), "child TSC.Enabled propagated from parent")
	s.Nil(childCfg.GetFastForwardConfig().GetDuration(), "MaxElapsedDuration never cascades into children")
	s.Empty(childCfg.GetFastForwardConfig().GetId(), "fast_forward_id is per-execution and never cascades into children")
	s.approxDuration(4*time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"child AccumulatedSkippedDuration == parent's accumulated at child-start (1h) + child's own skip for tc (3h) = 4h — exceeding the parent's 3h FastForward, possible only because the budget did not cascade")

	// The parent's StartChildWorkflowExecutionInitiated event must carry the propagated
	// TimeSkippingConfig and InitialSkippedDuration — these are what the transfer task
	// consumes when it starts the child.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 1)
	initAttrs := initEvents[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	initTSC := initAttrs.GetTimeSkippingConfig()
	s.NotNil(initTSC, "initiated event should carry TimeSkippingConfig snapshot")
	s.True(initTSC.GetEnabled(), "snapshot mirrors parent's Enabled flag")
	s.Nil(initTSC.GetFastForwardConfig().GetDuration(), "initiated-event snapshot excludes the parent's MaxElapsedDuration")
	s.Empty(initTSC.GetFastForwardConfig().GetId(), "initiated-event snapshot excludes the parent's fast_forward_id")
	s.approxDuration(time.Hour, initAttrs.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration(),
		"initiated event InitialSkippedDuration == parent's AccumulatedSkippedDuration at command time (1h)")
}

// TestTSPInChildWf_InheritsBudgetFreshCount verifies that a child inherits the parent's
// per-session budget (MaxSkipPerSession) but starts a fresh skip count. Children start
// internally, bypassing the frontend that populates the default budget — so without inheriting
// the parent's limit the child's config would carry MaxSkipPerSession=0 and time skipping would
// disable after its very first skip. The child here skips TWICE, which is only possible because
// it inherited the parent's budget (5) rather than 0.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_InheritsBudgetFreshCount() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childWFID := parentWFID + "-child"

	// A distinctive budget (not the dynamic-config default of 100) so the value observed on the
	// child unambiguously came from the parent's config. Small, but above each run's skip count.
	const maxSkip = 5

	startWall := time.Now()

	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: maxSkip},
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

	// The child skips twice: tc1 then tc2. Under the old bug (MaxSkipPerSession=0) skipping would
	// disable after tc1 and tc2 would never be skipped, hanging the test.
	childStarted, childSecond, childDone := false, false, false
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !childStarted {
			childStarted = true
			return cmdsResponse(timerCmd("tc1", 2*time.Hour)), nil
		}
		if fired["tc1"] && !childSecond {
			childSecond = true
			return cmdsResponse(timerCmd("tc2", 2*time.Hour)), nil
		}
		if fired["tc2"] && !childDone {
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
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than the virtual time skipped", elapsed)

	// ---- Child: inherited the budget, ran a fresh session, skipped twice ----
	childMS := s.getMutableStateByID(ctx, env, childWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childMS.State.ExecutionState.State)
	childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(childTSI)
	s.True(childTSI.GetConfig().GetEnabled(), "child skipping stayed enabled across both skips")
	s.Equal(int32(maxSkip), childTSI.GetConfig().GetMaxSessionSkipCount(),
		"child inherits the parent's per-session budget")
	s.Equal(int32(2), childTSI.GetSessionSkipCount(),
		"child ran a fresh session: only its own two skips, not the parent's count")

	// ---- Parent: its own budget and count are independent of the child's ----
	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, parentMS.State.ExecutionState.State)
	parentTSI := parentMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.Equal(int32(maxSkip), parentTSI.GetConfig().GetMaxSessionSkipCount())
	s.Equal(int32(2), parentTSI.GetSessionSkipCount(), "parent skipped t1 and t2")

	// The initiated event's TimeSkippingConfig snapshot carries the inherited budget but no count.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 1)
	initAttrs := initEvents[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	s.Equal(int32(maxSkip), initAttrs.GetTimeSkippingConfig().GetMaxSessionSkipCount(),
		"initiated-event snapshot carries the inherited budget")
	s.Equal(int32(0), initAttrs.GetTimeSkippingStatePropagation().GetInitialSkipCount(),
		"child does not inherit the parent's running skip count")
}

// TestTSPInChildWf_TwoChildren verifies that:
//   - All children started in the same WFT inherit the parent's TimeSkippingConfig
//     and have their AccumulatedSkippedDuration seeded to the parent's accumulated
//     skip at child-start time (via InitialSkippedDuration on the initiated event,
//     which applyTimeSkippingConfig converts into AccumulatedSkippedDuration).
//   - The parent does not skip while a child is still running. Once a child has
//     finished, its completion is held if the parent has an earlier timer, and
//     that timer fires before the completion is written.
//
// Scenario:
//   - Parent TSC enabled. Parent issues StartTimer(t1, 1h) → skips 1h.
//   - Parent issues [StartChild(c1), StartChild(c2), StartTimer(t2, 1h)].
//     Each child's MS is seeded with AccumulatedSkippedDuration = 1h
//     (inherited from the parent's accumulated at command time).
//   - Each child issues StartTimer(tc, 2h) → idle → skips 2h → completes.
//   - Both children close 2h after the parent started them. t2 is due in 1h,
//     so the parent fires t2 and completes without recording the child completions.
//   - Parent accumulated skip is still 1h (t1) + 1h (t2) = 2h.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_TwoChildren() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
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
		"parent skip: 1h (t1) + 1h (t2, due before either child finishes) = 2h")

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
		s.approxDuration(time.Hour, attrs.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration())
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
//   - P's pt2 is due at that same virtual time, so P fires pt2 and completes.
//     P.accum = 3h + 1h = 4h.
//   - G's gt2 was due 1h after P started, before P's close, so G fires gt2 and
//     completes. G.accum = 1h + 1h = 2h.
//
// Final state: C.accum == 4h (3h inherited + 1h own); P.accum == 4h; G.accum == 2h.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ThreeGenerations() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
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
		"G: 1h (gt1) + 1h (gt2, due before P finishes) = 2h")

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
	s.approxDuration(time.Hour, gInitAttrs.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration())

	pRunID := pMS.State.ExecutionState.RunId
	pInit := s.initiatedChildEvents(ctx, env, pWFID, pRunID)
	s.Len(pInit, 1)
	pInitAttrs := pInit[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	s.NotNil(pInitAttrs.GetTimeSkippingConfig())
	s.approxDuration(3*time.Hour, pInitAttrs.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration(),
		"P's initiated event for C carries P's full accumulated at command time (3h = G's 1h + P's pt1 2h)")
}

// TestTSPInChildWf_AdmissionTimestampsShifted verifies that a child workflow with a
// non-zero PropagatedSkippedDuration is admitted directly in the parent's virtual
// clock frame, while the WorkflowRunTimeoutTask's VisibilityTimestamp stays in the
// wall frame.
//
// Scenario:
//   - Parent TSC enabled. Parent runs StartTimer(t1, 1h) → idle → server skips 1h.
//     Parent's AccumulatedSkippedDuration ≈ 1h.
//   - Parent issues StartChildWorkflow with WorkflowRunTimeout=2h and
//     WorkflowExecutionTimeout=4h. The child's initiated event carries
//     InitialSkippedDuration=1h. On the child MS,
//     applyTimeSkippingConfig seeds AccumulatedSkippedDuration=1h, and the child
//     start request uses wallChildBoot+1h as its admission time.
//
// Boot-time assertions (virtual frame):
//   - executionState.StartTime ≈ wallChildBoot + 1h
//   - executionInfo.StartTime ≈ wallChildBoot + 1h
//   - executionInfo.ExecutionTime ≈ wallChildBoot + 1h (no first-WT delay)
//   - executionInfo.WorkflowRunExpirationTime ≈ wallChildBoot + 1h + 2h
//   - executionInfo.WorkflowExecutionExpirationTime ≈ wallChildBoot + 1h + 4h
//
// Wall-clock assertion (the load-bearing one): the child's WorkflowRunTimeoutTask
// must fire at wallChildBoot + 2h — i.e., virtual_expiration − accum — so a
// workflow that "should" run for 2h of virtual wall time still runs for 2h of real
// wall time, not less.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_AdmissionTimestampsShifted() {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithHistoryTaskRecorder(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
	)
	tv := testvars.New(s.T())
	ctx := s.Context()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childWFID := parentWFID + "-child"

	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	ns := env.Namespace().String()
	tq := tv.TaskQueue()

	// Bracket wallChildBoot tight around the parent WFT that issues the child command.
	// The child is admitted shortly after the response; with a 1-minute tolerance this
	// is a safe lower bound on the child's real-clock boot moment.
	var wallChildBoot time.Time

	parentStarted, parentKickedOff := false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case !parentStarted:
			parentStarted = true
			return cmdsResponse(timerCmd("t1", time.Hour)), nil
		case fired["t1"] && !parentKickedOff:
			parentKickedOff = true
			wallChildBoot = time.Now()
			childStartCmd := &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:                ns,
						WorkflowId:               childWFID,
						WorkflowType:             childWFType,
						TaskQueue:                tq,
						WorkflowExecutionTimeout: durationpb.New(4 * time.Hour),
						WorkflowRunTimeout:       durationpb.New(2 * time.Hour),
						WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
						ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
					},
				},
			}
			return cmdsResponse(childStartCmd), nil
		default:
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		}
	}

	childDone := false
	childHandler := func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !childDone {
			childDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	// Drive the parent until the child is started and completed; then we read the
	// child's MS to check boot-time admission timestamps. The parent itself is
	// terminated below so we don't have to wait for its 24h run timeout.
	for i := 0; i < 30 && !childDone; i++ {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, dispatch)
		if pollErr != nil {
			s.T().Logf("iter %d: poll error (likely timeout): %v", i, pollErr)
		}
	}
	s.True(childDone, "child workflow should have been driven to completion")
	s.False(wallChildBoot.IsZero(), "wallChildBoot should have been captured")

	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	parentTSI := parentMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(parentTSI)
	s.approxDuration(time.Hour, parentTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"parent should have accumulated ~1h of skip from t1 before issuing the child command")

	childMS := s.getMutableStateByID(ctx, env, childWFID)
	childRunID := childMS.State.ExecutionState.RunId
	childTSI := childMS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(childTSI)
	s.True(childTSI.GetConfig().GetEnabled(), "child should have TSC enabled propagated from parent")

	const accum = time.Hour
	const runTimeout = 2 * time.Hour
	const executionTimeout = 4 * time.Hour
	const tol = time.Minute

	expectedStart := wallChildBoot.Add(accum)
	s.WithinDuration(expectedStart, childMS.State.ExecutionState.StartTime.AsTime(), tol,
		"executionState.StartTime should be shifted forward by accum (1h) into the virtual frame")
	s.NotNil(childMS.State.ExecutionInfo.StartTime)
	s.WithinDuration(expectedStart, childMS.State.ExecutionInfo.StartTime.AsTime(), tol,
		"executionInfo.StartTime should be shifted forward by accum (1h) into the virtual frame")
	s.NotNil(childMS.State.ExecutionInfo.ExecutionTime)
	s.WithinDuration(expectedStart, childMS.State.ExecutionInfo.ExecutionTime.AsTime(), tol,
		"executionInfo.ExecutionTime equals StartTime (no first-WT delay) and is shifted by accum")
	s.NotNil(childMS.State.ExecutionInfo.WorkflowRunExpirationTime)
	expectedRunExpiration := wallChildBoot.Add(accum + runTimeout)
	s.WithinDuration(expectedRunExpiration, childMS.State.ExecutionInfo.WorkflowRunExpirationTime.AsTime(), tol,
		"WorkflowRunExpirationTime = StartTime + RunTimeout in the virtual frame")

	weExpiration := childMS.State.ExecutionInfo.WorkflowExecutionExpirationTime
	s.NotNil(weExpiration)
	expectedExecutionExpiration := wallChildBoot.Add(accum + executionTimeout)
	s.WithinDuration(expectedExecutionExpiration, weExpiration.AsTime(), tol,
		"transfer request must derive WorkflowExecutionExpirationTime from the virtual child start")

	recorder := env.GetTestCluster().GetHistoryTaskRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  childWFID,
		RunID:       childRunID,
	})
	var runTimeoutTask *historytasks.WorkflowRunTimeoutTask
	for _, rec := range recorded {
		if t, ok := rec.Task.(*historytasks.WorkflowRunTimeoutTask); ok {
			runTimeoutTask = t
			break
		}
	}
	s.NotNil(runTimeoutTask, "child should have a WorkflowRunTimeoutTask recorded")
	// virtual_expiration (childBoot + accum + runTimeout) − accum = childBoot + runTimeout;
	// confirms toRealTime converts back to the wall frame for task firing.
	expectedWall := wallChildBoot.Add(runTimeout)
	s.WithinDuration(expectedWall, runTimeoutTask.VisibilityTimestamp, tol,
		"WorkflowRunTimeoutTask must fire at wall-clock = virtual_expiration − accum (so the child still runs 2h of real time)")

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: parentWFID, RunId: parentRunID},
		Reason:            "test cleanup: stop parent",
	})
	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID, RunId: childRunID},
		Reason:            "test cleanup: stop child",
	})
}

// approxDuration asserts actual is within 10s of expected. Each skip loses
// (event_time - command_apply_time) of wall clock because
// AccumulatedSkippedDuration is computed as TargetTime - event.EventTime in
// ApplyWorkflowExecutionTimeSkippingTransitionedEvent; that drift accrues per
// skip and across nested workflows.
func (s *TimeSkippingPropagationTestSuite) approxDuration(expected, actual time.Duration, msgAndArgs ...any) {
	s.InDelta(float64(expected), float64(actual), float64(10*time.Second), msgAndArgs...)
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
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(s.Context(), &persistence.GetWorkflowExecutionRequest{
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

// childTerminalInTask reports a child terminal event in the events new to this workflow task.
func childTerminalInTask(task *workflowservice.PollWorkflowTaskQueueResponse) bool {
	events := task.History.Events
	from := int(task.PreviousStartedEventId)
	if from < 0 || from > len(events) {
		from = 0
	}
	for _, event := range events[from:] {
		if isChildTerminalEvent(event.GetEventType()) {
			return true
		}
	}
	return false
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
	return childCmdWithRunTimeout(ns, wfID, wfType, tq, 24*time.Hour)
}

func childCmdWithRunTimeout(ns, wfID string, wfType *commonpb.WorkflowType, tq *taskqueuepb.TaskQueue, runTimeout time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				Namespace:           ns,
				WorkflowId:          wfID,
				WorkflowType:        wfType,
				TaskQueue:           tq,
				WorkflowRunTimeout:  durationpb.New(runTimeout),
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

// activityCmd schedules an activity whose timeouts are short enough that skipping
// an hour past it would time the activity out.
func activityCmd(tv *testvars.TestVars, timeout time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             tv.ActivityID(),
				ActivityType:           tv.ActivityType(),
				TaskQueue:              tv.TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(timeout),
				ScheduleToStartTimeout: durationpb.New(timeout),
				StartToCloseTimeout:    durationpb.New(timeout),
			},
		},
	}
}

// signalExternalCmd signals another workflow in the same namespace by workflow ID.
func signalExternalCmd(ns, wfID, signalName string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
			SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace:  ns, //nolint:staticcheck // SA1019: the signal command still addresses its target with Namespace.
				Execution:  &commonpb.WorkflowExecution{WorkflowId: wfID},
				SignalName: signalName,
			},
		},
	}
}

// completedChildWorkflowIDs returns workflow IDs of child terminal events new to this task.
func completedChildWorkflowIDs(task *workflowservice.PollWorkflowTaskQueueResponse) map[string]bool {
	out := map[string]bool{}
	events := task.History.Events
	from := int(task.PreviousStartedEventId)
	if from < 0 || from > len(events) {
		from = 0
	}
	for _, event := range events[from:] {
		var execution *commonpb.WorkflowExecution
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			execution = event.GetChildWorkflowExecutionCompletedEventAttributes().GetWorkflowExecution()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			execution = event.GetChildWorkflowExecutionFailedEventAttributes().GetWorkflowExecution()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			execution = event.GetChildWorkflowExecutionCanceledEventAttributes().GetWorkflowExecution()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			execution = event.GetChildWorkflowExecutionTerminatedEventAttributes().GetWorkflowExecution()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			execution = event.GetChildWorkflowExecutionTimedOutEventAttributes().GetWorkflowExecution()
		default:
		}
		if execution != nil {
			out[execution.GetWorkflowId()] = true
		}
	}
	return out
}

// hasSignal reports whether this task delivered a workflow signal of the given name.
func hasSignal(task *workflowservice.PollWorkflowTaskQueueResponse, name string) bool {
	events := task.History.Events
	from := int(task.PreviousStartedEventId)
	if from < 0 || from > len(events) {
		from = 0
	}
	for _, event := range events[from:] {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED &&
			event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName() == name {
			return true
		}
	}
	return false
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
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	wfID := tv.WorkflowID()

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: false},
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
			TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true},
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
	s.WithinDuration(startedEvent.GetEventTime().AsTime(), resetMS.State.ExecutionState.GetStartTime().AsTime(), 10*time.Second,
		"reset/rebuild must preserve the already-materialized history start time")
	s.WithinDuration(startedEvent.GetEventTime().AsTime(), resetMS.State.ExecutionInfo.GetStartTime().AsTime(), 10*time.Second)
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
// DisablePropagation gates child propagation only — CaN ignores it — so run 1 sets
// it to verify the continuation still inherits Enabled=true.
//
// Scenario:
//   - Run 1 starts with TimeSkippingConfig{Enabled: true, DisablePropagation: true}.
//   - Run 1 issues StartTimer(t1, 1h). Idle → server skips 1h.
//     After skip: run1.AccumulatedSkippedDuration = 1h.
//   - t1 fires. Run 1 issues ContinueAsNew (same type, same task queue).
//   - Run 2 starts. Its WorkflowExecutionStarted event carries the current config
//     (Enabled=true, DisablePropagation=true) and InitialSkippedDuration=1h. The
//     applied MS has Config.Enabled=true and AccumulatedSkippedDuration=1h.
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
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
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
	s.True(run2Cfg.GetEnabled(),
		"run 2 TSC.Enabled propagated from run 1")
	s.approxDuration(3*time.Hour, run2TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"run 2 AccumulatedSkippedDuration == run 1's accumulated (1h) + run 2's own skip for t2 (2h) = 3h")

	// Run 2's WorkflowExecutionStarted event must carry the propagated TSC snapshot.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedEventTime := hist2.History.Events[0].GetEventTime().AsTime()
	s.WithinDuration(startedEventTime, run2MS.State.ExecutionState.GetStartTime().AsTime(), 10*time.Second,
		"continue-as-new constructor time is already virtual and must not be shifted again")
	s.WithinDuration(startedEventTime, run2MS.State.ExecutionInfo.GetStartTime().AsTime(), 10*time.Second)
	s.WithinDuration(startedEventTime, run2MS.State.ExecutionInfo.GetExecutionTime().AsTime(), 10*time.Second)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	s.Equal(run1ID, startedAttr.GetContinuedExecutionRunId(),
		"run 2 should reference run 1 as its predecessor")
	startedTSC := startedAttr.GetTimeSkippingConfig()
	s.NotNil(startedTSC, "run 2's WorkflowExecutionStarted must carry the TSC snapshot")
	s.True(startedTSC.GetEnabled(), "started event TSC.Enabled mirrors run 1's config")
	s.approxDuration(time.Hour, startedAttr.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration(),
		"started event retains InitialSkippedDuration=1h (run 1's accumulated at CaN time); applyTimeSkippingConfig uses it to seed AccumulatedSkippedDuration on the MS but the event snapshot is unchanged")
}

// TestTSPInCaN_SkipSessionPropagated verifies that continue-as-new carries the whole skip
// session forward: the per-session budget (MaxSkipPerSession, via the cloned config) AND the
// running SessionSkipCount (via InitialSkipCount). Run 1 skips once (count 1) then CaNs with a
// budget of 10; run 2 boots with the count seeded to 1, skips once more (count 2), and still
// sees the same budget.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCaN_SkipSessionPropagated() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	// A distinctive budget (not the dynamic-config default of 100) so a propagated value is
	// unambiguously the one from run 1's config. 10 is high enough that neither run hits the cap.
	const maxSkip = 10

	start1, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          wfID,
		WorkflowType:        wfType,
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: maxSkip},
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

	// ---- Run 1: skipped once (t1), CaN'd with the budget still enabled ----
	run1MS := s.getMutableState(env, wfID, run1ID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, run1MS.State.ExecutionState.Status)
	run1TSI := run1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run1TSI)
	s.Equal(int32(maxSkip), run1TSI.GetConfig().GetMaxSessionSkipCount())
	s.Equal(int32(1), run1TSI.GetSessionSkipCount(), "run 1 skipped once for t1")

	// ---- Run 2: inherits both the budget and the running count ----
	run2MS := s.getMutableStateByID(ctx, env, wfID)
	run2ID := run2MS.State.ExecutionState.RunId
	s.NotEqual(run1ID, run2ID, "run 2 should have a new run ID")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, run2MS.State.ExecutionState.Status)
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI, "run 2 should have TimeSkippingInfo propagated from run 1")
	s.Equal(int32(maxSkip), run2TSI.GetConfig().GetMaxSessionSkipCount(),
		"run 2 inherits the same per-session budget from run 1's cloned config")
	s.Equal(int32(2), run2TSI.GetSessionSkipCount(),
		"run 2 count = 1 inherited from run 1 + 1 for its own skip (t2)")

	// Run 2's WorkflowExecutionStarted event carries both in the propagated snapshot.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	s.Equal(int32(maxSkip), startedAttr.GetTimeSkippingConfig().GetMaxSessionSkipCount(),
		"started event TSC carries the propagated budget")
	s.Equal(int32(1), startedAttr.GetTimeSkippingStatePropagation().GetInitialSkipCount(),
		"started event carries run 1's SessionSkipCount as InitialSkipCount")
}

// TestTSPInCaN_ExecutionExpirationRemainsAbsolute verifies that time-skipping state
// propagation does not extend the workflow execution timeout across runs. The expiration
// timestamp is already expressed in virtual time and must remain unchanged when the next
// run inherits the accumulated skipped duration.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCaN_ExecutionExpirationRemainsAbsolute() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const executionTimeout = 3 * time.Hour
	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	start, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               wfID,
		WorkflowType:             wfType,
		TaskQueue:                tq,
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowRunTimeout:       durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		TimeSkippingConfig:       &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	run1ID := start.RunId

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
			return cmdsResponse(), nil
		}
		return cmdsResponse(), nil
	}

	for i := range 3 {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
		if pollErr != nil {
			s.T().Logf("iter %d: poll error: %v", i, pollErr)
		}
	}

	var status enumspb.WorkflowExecutionStatus
	s.AwaitTrue(func() bool {
		desc, describeErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
		})
		if describeErr != nil {
			return false
		}
		status = desc.WorkflowExecutionInfo.Status
		return status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}, 15*time.Second, 100*time.Millisecond)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, status)

	run1MS := s.getMutableState(env, wfID, run1ID)
	originalExpiration := run1MS.State.ExecutionInfo.GetWorkflowExecutionExpirationTime().AsTime()
	s.False(originalExpiration.IsZero())

	run2MS := s.getMutableStateByID(ctx, env, wfID)
	run2ID := run2MS.State.ExecutionState.RunId
	s.NotEqual(run1ID, run2ID)
	s.Equal(originalExpiration, run2MS.State.ExecutionInfo.GetWorkflowExecutionExpirationTime().AsTime(),
		"continue-as-new must preserve the chain's absolute virtual execution-expiration deadline")

	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	terminalEvent := hist2.History.Events[len(hist2.History.Events)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, terminalEvent.GetEventType())
	s.WithinDuration(originalExpiration, terminalEvent.GetEventTime().AsTime(), 10*time.Second,
		"workflow must time out at the original absolute virtual deadline")
}

// TestTSPInRetry verifies that a retried run is a same-lineage continuation: it inherits
// the previous attempt's current TimeSkippingConfig AND its in-flight accumulated skip.
//
// Retry snapshots from the previous run's live executionInfo (via
// snapshotTimeSkippingInfoForExecutionChain), not event #1: createRequest.TimeSkippingConfig
// carries the current config (including FastForward, ignoring DisablePropagation) and
// req.InitialSkippedDuration carries the previous attempt's AccumulatedSkippedDuration at
// retry time. initTimeSkippingInfo then seeds the retry's AccumulatedSkippedDuration from
// it — so the previous attempt's in-flight skip IS carried forward, exactly like
// continue-as-new.
//
// Scenario:
//   - Start a top-level workflow with TimeSkippingConfig{Enabled: true, FastForward: 4h}
//     and RetryPolicy{MaximumAttempts: 2}. Event #1's InitialSkippedDuration is unset.
//   - Attempt 1 issues StartTimer(t1, 1h). Idle → server skips 1h → attempt1.Accum = 1h.
//   - Attempt 1 issues FailWorkflowExecution with a bare (retryable) Failure.
//   - Server schedules retry. Attempt 2 (retry run) starts with InitialSkippedDuration=1h
//     (attempt 1's accumulated at retry time) → Accum seeded to 1h.
//   - Attempt 2 issues StartTimer(t2, 2h). Idle → server skips 2h → attempt2.Accum = 3h.
//   - Attempt 2 completes.
//
// End-state assertions:
//   - Attempt 1: status=FAILED, Config.Enabled=true, Accum=1h.
//   - Attempt 2: status=COMPLETED, Config.Enabled=true, FastForward=4h inherited
//     (the chain shares one budget), Accum=3h (1h inherited + own 2h) within that budget.
//   - Attempt 2's WorkflowExecutionStarted carries the current TSC snapshot (Enabled=true,
//     FastForward=4h) with InitialSkippedDuration≈1h and references attempt 1 as
//     ContinuedExecutionRunId.
func (s *TimeSkippingPropagationTestSuite) TestTSPInRetry() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		// FastForward is part of the inherited config: it caps the whole chain of attempts.
		TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(4 * time.Hour), Id: "ff-id"},
		},
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
	s.True(attempt2Cfg.GetEnabled(), "attempt 2 TSC.Enabled propagated from attempt 1's current config")
	s.Equal(4*time.Hour, attempt2Cfg.GetFastForwardConfig().GetDuration().AsDuration(),
		"attempt 2 inherits MaxElapsedDuration verbatim — the budget caps the whole chain, not each attempt")
	s.Equal("ff-id", attempt2Cfg.GetFastForwardConfig().GetId(),
		"attempt 2 inherits fast_forward_id — the fast-forward continues across the retry chain")
	s.approxDuration(3*time.Hour, attempt2TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"attempt 2 carries attempt 1's 1h in-flight skip forward plus its own 2h from t2 = 3h")
	s.LessOrEqual(attempt2TSI.GetAccumulatedSkippedDuration().AsDuration(), 4*time.Hour,
		"cap-total: the chain's accumulated skip stays within the shared MaxElapsedDuration budget")

	// Attempt 2's WorkflowExecutionStarted must carry the same TSC snapshot as attempt 1's event #1.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: attempt2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	expectedAttempt2Start := hist2.History.Events[0].GetEventTime().AsTime()
	s.WithinDuration(expectedAttempt2Start, attempt2MS.State.ExecutionState.GetStartTime().AsTime(), 10*time.Second,
		"WFT-completion retry event and mutable state must be constructed in the same virtual frame")
	s.WithinDuration(expectedAttempt2Start, attempt2MS.State.ExecutionInfo.GetStartTime().AsTime(), 10*time.Second)
	s.Equal(attempt1ID, startedAttr.GetContinuedExecutionRunId(),
		"attempt 2 references attempt 1 as its predecessor via ContinuedExecutionRunId")
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY, startedAttr.GetInitiator(),
		"attempt 2 is initiated by retry")
	startedTSC := startedAttr.GetTimeSkippingConfig()
	s.NotNil(startedTSC, "attempt 2's WorkflowExecutionStarted must carry the TSC snapshot")
	s.True(startedTSC.GetEnabled(), "started event TSC.Enabled mirrors attempt 1's current config")
	s.Equal(4*time.Hour, startedTSC.GetFastForwardConfig().GetDuration().AsDuration(),
		"started event carries the inherited MaxElapsedDuration — the snapshot of attempt 1's current config")
	s.Equal("ff-id", startedTSC.GetFastForwardConfig().GetId(),
		"started event carries the inherited fast_forward_id from attempt 1's current config")
	s.approxDuration(time.Hour, startedAttr.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration(),
		"started event InitialSkippedDuration carries attempt 1's in-flight accumulated skip (1h) at retry time")
}

// TestTSPInCron verifies that a cron-scheduled next run is a same-lineage continuation:
// it inherits the previous run's current TimeSkippingConfig (Enabled + FastForward) AND
// its in-flight accumulated skip.
//
// Cron and retry share SetupNewWorkflowForRetryOrCron, which snapshots from the previous
// run's live executionInfo (via snapshotTimeSkippingInfoForExecutionChain): the current
// config (incl. FastForward) and its AccumulatedSkippedDuration. initTimeSkippingInfo seeds
// the new run's AccumulatedSkippedDuration from that InitialSkippedDuration — so the
// previous run's in-flight skip IS carried forward, like continue-as-new. The FastForward
// is shared across the lineage (cap-total): run 2 inherits ~50min of the 1h budget,
// leaving only ~10min of skip for itself.
//
// Scenario:
//   - Start a cron workflow (`* * * * *`) with TimeSkippingConfig{Enabled: true,
//     MaxElapsedDuration: 1h}. Event #1's InitialSkippedDuration is unset.
//   - Run 1 issues StartTimer(t1, 50min). Idle → server skips 50min → run1.Accum ≈ 50min.
//   - Run 1 completes. Cron rolls forward; server creates run 2 with
//     createRequest.TimeSkippingConfig = run 1's current config (Enabled=true,
//     MaxElapsedDuration=1h) and InitialSkippedDuration ≈ 50min → run 2 MS Accum seeded ≈ 50min.
//
// End-state assertions:
//   - Run 1: Config{Enabled, FastForward=1h}, Accum ≈ 50min.
//   - Run 2: Config{Enabled, FastForward=1h} (inherited), Accum ≈ 50min carried forward
//     and never exceeding the 1h cap-total budget.
//   - Run 2's WorkflowExecutionStarted carries the current TSC snapshot with
//     InitialSkippedDuration ≈ 50min, initiator=CRON_SCHEDULE.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCron() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	wfType := tv.WorkflowType()
	wfID := tv.WorkflowID()
	tq := tv.TaskQueue()

	inputCfg := &commonpb.TimeSkippingConfig{
		Enabled:             true,
		MaxSessionSkipCount: 50,
		FastForwardConfig:   &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour), Id: uuid.NewString()},
	}

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

	// ---- Run 1: Config{Enabled, FastForward=1h}, Accum ≈ 50min ----
	run1MS := s.getMutableState(env, wfID, run1ID)
	run1TSI := run1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run1TSI)
	s.True(proto.Equal(inputCfg, run1TSI.GetConfig()),
		"run 1 Config mirrors the start request: Enabled=true, MaxElapsedDuration=1h")
	// Tolerance absorbs per-skip clock drift plus up to ~60s of cron backoff that may
	// be skipped while waiting for the first WFT.
	s.InDelta(float64(50*time.Minute), float64(run1TSI.GetAccumulatedSkippedDuration().AsDuration()), float64(90*time.Second),
		"run 1 skipped ~50min for t1 before cron rolled it forward")

	// ---- Run 2: Config inherited (Enabled + FastForward); in-flight skip carried forward ----
	run2MS := s.getMutableState(env, wfID, run2ID)
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI, "run 2 should have TimeSkippingInfo propagated from run 1's current config")
	s.True(proto.Equal(inputCfg, run2TSI.GetConfig()),
		"run 2 Config matches run 1's current config: Enabled=true and MaxElapsedDuration=1h are inherited")
	s.GreaterOrEqual(run2TSI.GetAccumulatedSkippedDuration().AsDuration(), 45*time.Minute,
		"run 2 carries run 1's ~50min in-flight skip forward (cron is a continuation)")
	s.LessOrEqual(run2TSI.GetAccumulatedSkippedDuration().AsDuration(), time.Hour+time.Minute,
		"cap-total: run 2's accumulated skip never exceeds the inherited MaxElapsedDuration=1h budget")

	// Run 2's WorkflowExecutionStarted carries the verbatim TSC snapshot from run 1's event #1.
	hist2, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
	})
	s.NoError(err)
	s.NotEmpty(hist2.History.Events)
	startedAttr := hist2.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(startedAttr)
	expectedRun2Start := hist2.History.Events[0].GetEventTime().AsTime()
	s.WithinDuration(expectedRun2Start, run2MS.State.ExecutionState.GetStartTime().AsTime(), 90*time.Second,
		"cron event and mutable state must be constructed in the same virtual frame")
	s.WithinDuration(expectedRun2Start, run2MS.State.ExecutionInfo.GetStartTime().AsTime(), 90*time.Second)
	s.Equal(run1ID, startedAttr.GetContinuedExecutionRunId(),
		"run 2 references run 1 as its predecessor via ContinuedExecutionRunId")
	s.Equal(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE, startedAttr.GetInitiator(),
		"run 2 is initiated by cron schedule")
	s.True(proto.Equal(inputCfg, startedAttr.GetTimeSkippingConfig()),
		"started event carries Enabled=true, MaxElapsedDuration=1h — the snapshot from run 1's current config")
	s.InDelta(float64(50*time.Minute), float64(startedAttr.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration()), float64(90*time.Second),
		"started event InitialSkippedDuration carries run 1's in-flight accumulated skip at cron-rollover time")
}

// TestTSPInCaN verifies end-to-end that one FastForward budget caps
// the whole chain of runs: a continuation inherits the budget already consumed by its
// predecessor, may only skip the remainder, and consuming it disables time skipping.
//
// initTimeSkippingInfo applies the full-cap anchor on a new run: the fast-forward target
// is now + (FastForward - inherited AccumulatedSkippedDuration). Run 2 therefore boots
// with only 1h of the 3h budget left, even though it never skipped anything itself.
//
// Scenario:
//   - Run 1 starts with TimeSkippingConfig{Enabled: true, FastForward: 3h}.
//   - Run 1 issues StartTimer(t1, 2h). Idle → skips 2h (budget not reached) →
//     run1.Accum = 2h. t1 fires; run 1 continues-as-new.
//   - Run 2 boots with Config{Enabled, FastForward=3h} and Accum seeded to 2h →
//     remaining budget 1h.
//   - Run 2's first WFT completes with no commands. Run 2 is idle with the fast-forward
//     target as its only skip candidate → skips the remaining 1h, reaches the target,
//     and time skipping is disabled (DisabledAfterFastForward=true). run2.Accum = 3h —
//     the full chain budget, of which run 2 itself contributed only 1h.
//
// End-state assertions:
//   - run1: CONTINUED_AS_NEW, Config.Enabled=true at CaN time (budget not reached),
//     FastForward not reached, Accum ≈ 2h.
//   - run2: Config.Enabled=false, FastForward.HasReached=true, Accum ≈ 3h.
//   - run2 history has exactly one transition, carrying DisabledAfterFastForward=true.
func (s *TimeSkippingPropagationTestSuite) TestTSPInCaN_BudgetCapOverChain() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled:           true,
			FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(3 * time.Hour), Id: "ff-id"},
		},
	})
	s.NoError(err)
	run1ID := start1.RunId

	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case state == 0:
			state = 1
			return cmdsResponse(timerCmd("t1", 2*time.Hour)), nil
		case state == 1 && fired["t1"]:
			state = 2
			return cmdsResponse(continueAsNewCmd(wfType, tq)), nil
		}
		// Run 2's WFTs: no commands — run 2 idles so the fast-forward target is its
		// only skip candidate.
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	// Drive run 1 to CONTINUED_AS_NEW.
	s.drivePollsUntilRunNotRunning(ctx, env, tv, handler, wfID, run1ID, 20)

	run2MS := s.getMutableStateByID(ctx, env, wfID)
	run2ID := run2MS.State.ExecutionState.RunId
	s.NotEqual(run1ID, run2ID, "CaN should have created a new run")

	// Serve run 2's first WFT with an empty response; its close-tx skips the remaining
	// budget and disables time skipping synchronously.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
	s.NoError(err)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 3h of virtual time", elapsed)

	// ---- Run 1: consumed 2h of the budget but never reached it ----
	run1MS := s.getMutableState(env, wfID, run1ID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, run1MS.State.ExecutionState.Status)
	run1TSI := run1MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run1TSI)
	s.True(run1TSI.GetConfig().GetEnabled(),
		"run 1 hands the config over still enabled — it consumed only 2h of the 3h budget")
	s.False(run1TSI.GetFastForwardInfo().GetHasReached(),
		"run 1 must not have reached the fast-forward target")
	s.approxDuration(2*time.Hour, run1TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"run 1 skipped 2h for t1 before continue-as-new")

	// ---- Run 2: inherits 2h consumed, skips only the remaining 1h, then disables ----
	run2MS = s.getMutableState(env, wfID, run2ID)
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI, "run 2 should have TimeSkippingInfo propagated from run 1")
	s.False(run2TSI.GetConfig().GetEnabled(),
		"the chain's budget is exhausted in run 2 → time skipping disabled")
	s.True(run2TSI.GetFastForwardInfo().GetHasReached(),
		"run 2 reached the fast-forward target inherited from the chain")
	s.approxDuration(3*time.Hour, run2TSI.GetAccumulatedSkippedDuration().AsDuration(),
		"chain total: run 1 contributed 2h, run 2 only the remaining 1h — the budget caps the chain, not each run")

	// Run 2's history carries exactly one transition: the cap-hit skip+disable.
	hist2 := env.RequireHistory(s, &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID})
	var transitions []*historypb.HistoryEvent
	for _, e := range hist2 {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			transitions = append(transitions, e)
		}
	}
	s.Len(transitions, 1, "run 2 skips exactly once: the remaining budget")
	if len(transitions) == 1 {
		attrs := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
		s.True(attrs.GetDisabledAfterFastForward(),
			"run 2's only transition is the fast-forward disable")
	}

	// Run 2 is idle and RUNNING with skipping disabled; stop it.
	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run2ID},
		Reason:            "test cleanup",
	})
}

// TestTSPInChildWf_PropagationDisabled verifies that DisablePropagation suppresses the
// TimeSkippingConfig from cascading into children while the parent's accumulated virtual
// time is still inherited. The child ends up with Config.Enabled == false (it never skips
// its own timers) but AccumulatedSkippedDuration seeded from the parent.
//
// Scenario:
//   - Parent starts with TimeSkippingConfig{Enabled: true, DisablePropagation: true}.
//   - Parent issues StartTimer(t1, 1h) → idle → skips 1h → parent.Accum = 1h.
//   - t1 fires. Parent issues StartChild(child) + StartTimer(t2, 1h). Because propagation
//     is disabled, the child gets no TimeSkippingConfig, but its TSI still carries the
//     inherited InitialSkippedDuration = 1h.
//   - Child has skipping disabled (Config nil → Enabled false), so it does not skip its own
//     time; it completes immediately without a long timer.
//   - Child completes → parent becomes idle on t2 → skips 1h → t2 fires → parent completes.
//
// End-state assertions:
//   - parent.AccumulatedSkippedDuration == 2h (1h for t1 + 1h for t2).
//   - child.TimeSkippingInfo.Config is nil (Enabled == false); propagation was disabled.
//   - child.AccumulatedSkippedDuration == 1h (inherited only; the child never skips).
//   - The parent's StartChildWorkflowExecutionInitiated event carries no TimeSkippingConfig
//     but InitialSkippedDuration == 1h.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_PropagationDisabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		// DisablePropagation keeps the enabled flag from cascading into the child; only the
		// parent's accumulated virtual time is inherited.
		TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled:            true,
			DisablePropagation: true,
		},
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

	// The child has skipping disabled, so it must not wait on a long timer (nothing would
	// skip it). It completes on its first workflow task.
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return cmdsResponse(completeCmd()), nil
	}

	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	s.drivePollsUntilClosed(ctx, env, tv, dispatch, parentWFID, parentRunID, 20)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 2h of virtual time", elapsed)

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
	s.NotNil(childTSI, "child still gets a TimeSkippingInfo to carry inherited virtual time")
	s.Nil(childTSI.GetConfig(), "DisablePropagation suppresses the config; child has no TimeSkippingConfig")
	s.False(childTSI.GetConfig().GetEnabled(), "child skipping is disabled")
	s.approxDuration(time.Hour, childTSI.GetAccumulatedSkippedDuration().AsDuration(),
		"child AccumulatedSkippedDuration == parent's accumulated at child-start (1h); the child never skips its own time")

	// The parent's StartChildWorkflowExecutionInitiated event carries no TimeSkippingConfig
	// (propagation disabled) but still snapshots the parent's accumulated skip so the child
	// inherits virtual time.
	initEvents := s.initiatedChildEvents(ctx, env, parentWFID, parentRunID)
	s.Len(initEvents, 1)
	initAttrs := initEvents[0].GetStartChildWorkflowExecutionInitiatedEventAttributes()
	s.Nil(initAttrs.GetTimeSkippingConfig(), "initiated event carries no TimeSkippingConfig when propagation is disabled")
	s.approxDuration(time.Hour, initAttrs.GetTimeSkippingStatePropagation().GetInitialSkippedDuration().AsDuration(),
		"initiated event InitialSkippedDuration == parent's AccumulatedSkippedDuration at command time (1h)")
}

// TestTSPInChildWf_ParentRunTimeoutBeforeChild is the run-timeout case: the parent
// run timeout is 1h and the child sleeps 2h. Real time times the parent out at 1h.
// The child completion must not be recorded first.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ParentRunTimeoutBeforeChild() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childWFID := parentWFID + "-child"

	startWall := time.Now()
	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               parentWFID,
		WorkflowType:             parentWFType,
		TaskQueue:                tv.TaskQueue(),
		WorkflowRunTimeout:       durationpb.New(time.Hour),
		WorkflowExecutionTimeout: durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		TimeSkippingConfig:       &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	parentStarted := false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(childCmdWithRunTimeout(env.Namespace().String(), childWFID, childWFType, tv.TaskQueue(), 3*time.Hour)), nil
		}
		// ChildWorkflowExecutionStarted is not a completion. Completing here would
		// close the parent before its run timeout. A child terminal event means
		// the timeout lost the race, so close and let the status assertion fail.
		if childTerminalInTask(task) {
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	childStarted, childDone := false, false
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !childStarted {
			childStarted = true
			return cmdsResponse(timerCmd("sleep", 2*time.Hour)), nil
		}
		if fired["sleep"] && !childDone {
			childDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})
	s.drivePollsUntilRunNotRunning(ctx, env, tv, dispatch, parentWFID, parentRunID, 20)

	elapsed := time.Since(startWall)
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than the 1h run timeout", elapsed)
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: parentWFID, RunId: parentRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.WorkflowExecutionInfo.Status)
	s.False(s.historyHasEvent(ctx, env, parentWFID, parentRunID, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED),
		"the parent times out before the child completion is recorded")
}

// TestTSPInChildWf_ParentTimerBeforeChild is the timer race: the parent's 30m timer
// and a child that sleeps 1h are started together. The timer fires first.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ParentTimerBeforeChild() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	parentStarted, parentDone := false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(
				timerCmd("parent", 30*time.Minute),
				childCmd(env.Namespace().String(), childWFID, childWFType, tv.TaskQueue()),
			), nil
		}
		if !parentDone && (fired["parent"] || childTerminalInTask(task)) {
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
			return cmdsResponse(timerCmd("sleep", time.Hour)), nil
		}
		if fired["sleep"] && !childDone {
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
	s.Less(elapsed, 2*time.Minute, "wall-clock elapsed (%s) should be far less than 1h of virtual time", elapsed)
	s.True(s.timerFiredBeforeChildCompletion(ctx, env, parentWFID, parentRunID, "parent"),
		"the 30m timer fires before the child that sleeps 1h completes")
}

// TestTSPInChildWf_ChildBeforeParentTimer keeps the opposite order: a child that
// finishes before the parent's timer is recorded first, and the timer does not fire.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ChildBeforeParentTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childWFID := parentWFID + "-child"

	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	parentStarted, parentDone := false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(
				timerCmd("parent", 30*time.Minute),
				childCmd(env.Namespace().String(), childWFID, childWFType, tv.TaskQueue()),
			), nil
		}
		if !parentDone && (childTerminalInTask(task) || fired["parent"]) {
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
			return cmdsResponse(timerCmd("sleep", 10*time.Minute)), nil
		}
		if fired["sleep"] && !childDone {
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

	s.True(s.childCompletionBeforeTimer(ctx, env, parentWFID, parentRunID, "parent"),
		"the child that sleeps 10m is recorded before the parent's 30m timer fires")
}

// TestTSPInChildWf_ActivityInFlightDoesNotSkip is the running-activity case. The
// child finishes an hour ahead of the parent while an activity is still pending.
// The parent must not skip to the child's close time until that activity is done.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_ActivityInFlightDoesNotSkip() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

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
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	verifiedBlocked := false
	parentStarted, parentDone := false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(
				activityCmd(tv, 2*time.Minute),
				childCmd(env.Namespace().String(), childWFID, childWFType, tv.TaskQueue()),
			), nil
		}
		if verifiedBlocked && childTerminalInTask(task) && !parentDone {
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
			return cmdsResponse(timerCmd("sleep", time.Hour)), nil
		}
		if fired["sleep"] && !childDone {
			childDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	blocked := false
	for i := 0; i < 30 && !blocked; i++ {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, dispatch, taskpoller.WithTimeout(5*time.Second))
		if pollErr != nil {
			s.T().Logf("iter %d: poll error (likely timeout): %v", i, pollErr)
		}
		childDesc, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		})
		if descErr != nil || childDesc.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			continue
		}
		parentMS := s.getMutableState(env, parentWFID, parentRunID)
		accumulated := parentMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
		if len(parentMS.State.ActivityInfos) > 0 &&
			accumulated < time.Minute &&
			!s.historyHasEvent(ctx, env, parentWFID, parentRunID, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED) &&
			!s.historyHasEvent(ctx, env, parentWFID, parentRunID, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT) {
			blocked = true
		}
	}
	s.True(blocked, "the parent keeps the activity and does not skip to the child's close time while it is in flight")

	verifiedBlocked = true
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, func(_ *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{}, nil
	}, taskpoller.WithTimeout(10*time.Second))
	s.NoError(err)

	for i := 0; i < 20 && !parentDone; i++ {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, dispatch, taskpoller.WithTimeout(10*time.Second))
		if pollErr != nil {
			s.T().Logf("after activity: iter %d: poll error (likely timeout): %v", i, pollErr)
		}
		desc, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: parentWFID, RunId: parentRunID},
		})
		s.NoError(descErr)
		if desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			parentDone = true
		}
	}

	s.Less(time.Since(startWall), 2*time.Minute, "wall clock should stay far below the child's 1h virtual sleep")
	s.True(parentDone, "the parent completes after the activity, once the child completion is delivered")
	s.True(s.historyHasEvent(ctx, env, parentWFID, parentRunID, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED),
		"the activity completes on its own, rather than being skipped")
	s.True(s.historyHasEvent(ctx, env, parentWFID, parentRunID, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED))
	parentMS := s.getMutableState(env, parentWFID, parentRunID)
	accumulated := parentMS.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
	s.Greater(accumulated, 30*time.Minute, "once the activity is done the parent skips toward the child's close time")
	s.Less(accumulated, time.Hour+time.Minute)
}

// TestTSPInChildWf_SiblingSignalDoesNotDeadlock is the case where holding the
// child completion until the parent is skippable deadlocks. Child A finishes
// ahead of the parent. Child B waits for a signal the parent sends only after A
// is recorded. B stays in flight, so the parent cannot skip, but A's completion
// is still delivered when the parent's clock reaches A's close time.
func (s *TimeSkippingPropagationTestSuite) TestTSPInChildWf_SiblingSignalDoesNotDeadlock() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	parentWFType := tv.WorkflowType()
	childWFType := &commonpb.WorkflowType{Name: parentWFType.Name + "-child"}
	parentWFID := tv.WorkflowID()
	childAWFID := parentWFID + "-child-a"
	childBWFID := parentWFID + "-child-b"
	ns := env.Namespace().String()
	tq := tv.TaskQueue()

	startWall := time.Now()
	parentStart, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          parentWFID,
		WorkflowType:        parentWFType,
		TaskQueue:           tq,
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	parentRunID := parentStart.RunId

	parentStarted, signaled, parentDone := false, false, false
	parentHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !parentStarted {
			parentStarted = true
			return cmdsResponse(
				childCmd(ns, childAWFID, childWFType, tq),
				childCmd(ns, childBWFID, childWFType, tq),
			), nil
		}
		completed := completedChildWorkflowIDs(task)
		if completed[childAWFID] && !signaled {
			signaled = true
			return cmdsResponse(signalExternalCmd(ns, childBWFID, "go")), nil
		}
		if completed[childBWFID] && !parentDone {
			parentDone = true
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	childAStarted, childADone := false, false
	childBStarted, childBDone := false, false
	childHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch task.WorkflowExecution.GetWorkflowId() {
		case childAWFID:
			if !childAStarted {
				childAStarted = true
				return cmdsResponse(timerCmd("sleep", 10*time.Second)), nil
			}
			if fired["sleep"] && !childADone {
				childADone = true
				return cmdsResponse(completeCmd()), nil
			}
		case childBWFID:
			// An in-flight activity keeps B from skipping to its own run timeout while it
			// waits. B stays a running child, so the parent cannot skip either.
			if !childBStarted {
				childBStarted = true
				return cmdsResponse(activityCmd(tv, 10*time.Minute)), nil
			}
			if hasSignal(task, "go") && !childBDone {
				childBDone = true
				return cmdsResponse(completeCmd()), nil
			}
		default:
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	dispatch := typeDispatch(map[string]wftHandler{
		parentWFType.Name: parentHandler,
		childWFType.Name:  childHandler,
	})

	for i := 0; i < 12 && !parentDone; i++ {
		_, pollErr := env.TaskPoller().PollAndHandleWorkflowTask(tv, dispatch, taskpoller.WithTimeout(15*time.Second))
		if pollErr != nil {
			s.T().Logf("iter %d: poll error (likely timeout): %v", i, pollErr)
		}
		desc, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: parentWFID, RunId: parentRunID},
		})
		s.NoError(descErr)
		if desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			parentDone = true
		}
	}

	s.True(parentDone, "the sibling waiting on a signal must not deadlock the parent")
	s.Less(time.Since(startWall), time.Minute, "delivery waits for the parent's clock, not for an hour of virtual time")
	s.True(signaled, "the parent signals the sibling after the first child is recorded")
	s.True(s.historyHasEvent(ctx, env, childBWFID, "", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))
	childB := s.getMutableStateByID(ctx, env, childBWFID)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, childB.State.ExecutionState.State)
}

func (s *TimeSkippingPropagationTestSuite) historyEvents(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID string,
) []*historypb.HistoryEvent {
	var events []*historypb.HistoryEvent
	var token []byte
	for {
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       env.Namespace().String(),
			Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			MaximumPageSize: 1000,
			NextPageToken:   token,
		})
		s.NoError(err)
		events = append(events, resp.GetHistory().GetEvents()...)
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			return events
		}
	}
}

func (s *TimeSkippingPropagationTestSuite) historyHasEvent(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID string,
	eventType enumspb.EventType,
) bool {
	for _, event := range s.historyEvents(ctx, env, workflowID, runID) {
		if event.GetEventType() == eventType {
			return true
		}
	}
	return false
}

func (s *TimeSkippingPropagationTestSuite) timerFiredBeforeChildCompletion(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID, timerID string,
) bool {
	timerIdx, childIdx := -1, -1
	for i, event := range s.historyEvents(ctx, env, workflowID, runID) {
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED &&
			event.GetTimerFiredEventAttributes().GetTimerId() == timerID {
			timerIdx = i
		}
		if isChildTerminalEvent(event.GetEventType()) && childIdx < 0 {
			childIdx = i
		}
	}
	if timerIdx < 0 {
		return false
	}
	return childIdx < 0 || timerIdx < childIdx
}

func (s *TimeSkippingPropagationTestSuite) childCompletionBeforeTimer(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID, timerID string,
) bool {
	timerIdx, childIdx := -1, -1
	for i, event := range s.historyEvents(ctx, env, workflowID, runID) {
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED &&
			event.GetTimerFiredEventAttributes().GetTimerId() == timerID && timerIdx < 0 {
			timerIdx = i
		}
		if isChildTerminalEvent(event.GetEventType()) && childIdx < 0 {
			childIdx = i
		}
	}
	if childIdx < 0 {
		return false
	}
	return timerIdx < 0 || childIdx < timerIdx
}

func isChildTerminalEvent(eventType enumspb.EventType) bool {
	switch eventType {
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		return true
	default:
		return false
	}
}
