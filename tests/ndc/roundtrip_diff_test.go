package ndc

// Diff engine for the round-trip framework: normalization, the allowlist of legitimate
// active/passive divergences, and failure reporting.
//
// The allowlist is the point of this file. Active and passive do not generate identical
// tasks, and every place they differ is a deliberate design decision somewhere in the
// codebase. Each rule therefore carries a reason and a citation, and rules are scoped to a
// task type rather than a whole category wherever possible -- a category-wide waiver blinds
// the diff to everything in it.

import (
	"fmt"
	"sort"
	"strings"

	"go.temporal.io/server/service/history/tasks"
)

// rtSide identifies which cluster produced a task.
type rtSide string

const (
	rtActive  rtSide = "active"
	rtPassive rtSide = "passive"
)

// rtAllowRule waives a divergence. reason and ref are mandatory: a waiver without a stated
// justification and a code citation is indistinguishable from a bug someone silenced.
//
// knownDefect separates the two very different reasons a divergence is waived. Most rules
// describe places the two sides are *supposed* to differ. A knownDefect rule describes a
// passive-side bug this framework found, waived only so the suite stays green until it is
// fixed -- it is a TODO, not documentation of intended behavior, and should be deleted
// along with the fix. They are reported separately at teardown so they stay visible.
type rtAllowRule struct {
	name        string
	reason      string
	ref         string
	knownDefect bool
	match       func(side rtSide, identity string) bool
}

// rtExcludedCategories are dropped before the diff runs. Only categories whose divergence
// is total and by design belong here.
var rtExcludedCategories = map[tasks.Category]string{
	// The passive cluster generates no replication tasks at all; mutable_state_impl.go
	// closeTransactionPrepareReplicationTasks hard-asserts this.
	tasks.CategoryReplication: "passive generates no replication tasks by design",
	// Active emits a visibility upsert per transaction; the passive refresher emits one
	// coalesced task per replicated delta. Same terminal state, different multiplicity.
	tasks.CategoryVisibility: "multiplicity differs by design (per-transaction vs per-delta)",
}

// rtGlobalAllowlist holds divergences that can occur in any case.
var rtGlobalAllowlist = []rtAllowRule{
	{
		name:   "reset-workflow-task-active-only",
		reason: "closeTransactionHandleWorkflowResetTask runs only under TransactionPolicyActive",
		ref:    "service/history/workflow/mutable_state_impl.go closeTransactionHandleWorkflowResetTask",
		match: func(side rtSide, identity string) bool {
			return side == rtActive && strings.HasPrefix(identity, "*tasks.ResetWorkflowTask")
		},
	},
	{
		name:        "DEFECT-passive-duplicates-workflow-run-timeout-timer",
		knownDefect: true,
		reason: "The passive cluster creates a WorkflowRunTimeoutTask the active cluster never " +
			"creates, at the same deadline as the real one. RefreshTasksForWorkflowStart runs " +
			"on a partial refresh whenever execution state changed within the replicated " +
			"delta, and GenerateWorkflowStartTasks then regenerates the run timeout " +
			"unconditionally: on a first run isFirstRun is true, so the execution timeout " +
			"timer is never created, executionTimeoutTimerTaskStatus stays " +
			"TimerTaskStatusNone, and the `status == None` branch always fires. Note that " +
			"WorkflowExecutionTimerTaskStatus does NOT guard this -- gating the mask clearing " +
			"on full refresh (the refreshTasksForActivity idiom) does not fix it. The likely " +
			"fix is to skip RefreshTasksForWorkflowStart on a partial refresh entirely, since " +
			"every creation and rebuild path uses the full Refresh and so start tasks always " +
			"exist by then.",
		ref: "service/history/workflow/task_refresher.go RefreshTasksForWorkflowStart -> " +
			"task_generator.go GenerateWorkflowStartTasks",
		match: func(side rtSide, identity string) bool {
			return side == rtPassive &&
				(strings.HasPrefix(identity, "*tasks.WorkflowRunTimeoutTask") ||
					strings.HasPrefix(identity, "*tasks.WorkflowExecutionTimeoutTask"))
		},
	},
	{
		name:   "time-skipping-active-only",
		reason: "time-skipping transitions and timer re-stamping are active-only; passive re-derives them",
		ref:    "service/history/workflow/timeskipping.go closeTransactionHandleWorkflowTimeSkipping",
		match: func(side rtSide, identity string) bool {
			return side == rtActive && strings.Contains(identity, "TimeSkipping")
		},
	},
}

// rtNormalize projects a task down to what identifies it, dropping the fields the receiving
// shard assigns (TaskID, and VisibilityTimestamp for immediate tasks).
//
// This is a type switch rather than reflection on purpose: when a new task type appears,
// this test must be updated deliberately rather than silently comparing nothing.
func rtNormalize(task tasks.Task) string {
	switch t := task.(type) {
	case *tasks.ActivityTask:
		return fmt.Sprintf("*tasks.ActivityTask{scheduledEventID:%d taskQueue:%q stamp:%d}",
			t.ScheduledEventID, t.TaskQueue, t.Stamp)
	case *tasks.ActivityTimeoutTask:
		return fmt.Sprintf("*tasks.ActivityTimeoutTask{eventID:%d timeoutType:%v attempt:%d}",
			t.EventID, t.TimeoutType, t.Attempt)
	case *tasks.ActivityRetryTimerTask:
		return fmt.Sprintf("*tasks.ActivityRetryTimerTask{eventID:%d attempt:%d}", t.EventID, t.Attempt)
	case *tasks.UserTimerTask:
		return fmt.Sprintf("*tasks.UserTimerTask{eventID:%d}", t.EventID)
	case *tasks.WorkflowTask:
		return fmt.Sprintf("*tasks.WorkflowTask{scheduledEventID:%d taskQueue:%q}",
			t.ScheduledEventID, t.TaskQueue)
	case *tasks.WorkflowTaskTimeoutTask:
		return fmt.Sprintf("*tasks.WorkflowTaskTimeoutTask{eventID:%d timeoutType:%v scheduleAttempt:%d}",
			t.EventID, t.TimeoutType, t.ScheduleAttempt)
	case *tasks.StartChildExecutionTask:
		return fmt.Sprintf("*tasks.StartChildExecutionTask{initiatedEventID:%d}", t.InitiatedEventID)
	case *tasks.CancelExecutionTask:
		return fmt.Sprintf("*tasks.CancelExecutionTask{initiatedEventID:%d}", t.InitiatedEventID)
	case *tasks.SignalExecutionTask:
		return fmt.Sprintf("*tasks.SignalExecutionTask{initiatedEventID:%d}", t.InitiatedEventID)
	case *tasks.CloseExecutionTask:
		return fmt.Sprintf("*tasks.CloseExecutionTask{deleteAfterClose:%t}", t.DeleteAfterClose)
	case *tasks.DeleteHistoryEventTask:
		return "*tasks.DeleteHistoryEventTask{}"
	case *tasks.WorkflowRunTimeoutTask:
		return "*tasks.WorkflowRunTimeoutTask{}"
	case *tasks.WorkflowExecutionTimeoutTask:
		return fmt.Sprintf("*tasks.WorkflowExecutionTimeoutTask{firstRunID:%q}", t.FirstRunID)
	case *tasks.WorkflowBackoffTimerTask:
		return fmt.Sprintf("*tasks.WorkflowBackoffTimerTask{type:%v}", t.WorkflowBackoffType)
	case *tasks.ResetWorkflowTask:
		return "*tasks.ResetWorkflowTask{}"
	case *tasks.StateMachineOutboundTask:
		return fmt.Sprintf("*tasks.StateMachineOutboundTask{type:%q}", t.Info.GetType())
	case *tasks.StateMachineTimerTask:
		return "*tasks.StateMachineTimerTask{}"
	default:
		return fmt.Sprintf("UNHANDLED(%T)", task)
	}
}

// diffTasks compares the transfer and timer tasks the two sides produced.
func (s *rtSuite) diffTasks(
	step rtStep,
	activeTasks map[tasks.Category][]tasks.Task,
	passiveTasks map[tasks.Category][]tasks.Task,
) {
	compared := 0
	for _, category := range []tasks.Category{tasks.CategoryTransfer, tasks.CategoryTimer} {
		compared += len(activeTasks[category]) + len(passiveTasks[category])
		s.diffCategory(step, category, activeTasks[category], passiveTasks[category])
	}

	// A step where neither side produced anything compares nothing and passes for free.
	// That is almost always a broken fixture rather than a real "no tasks" outcome, so make
	// the step opt into it explicitly.
	if !step.allowNoTasks {
		s.NotZero(compared,
			"step %q produced no transfer or timer tasks on either side, so the diff was "+
				"vacuous; set allowNoTasks on the step if that is genuinely expected", step.name)
	}
}

func (s *rtSuite) diffCategory(
	step rtStep,
	category tasks.Category,
	activeTasks []tasks.Task,
	passiveTasks []tasks.Task,
) {
	activeIdentities := s.normalizeAll(activeTasks)
	passiveIdentities := s.normalizeAll(passiveTasks)

	activeOnly, passiveOnly, matched := rtDiffMultisets(activeIdentities, passiveIdentities)

	rules := append(append([]rtAllowRule(nil), rtGlobalAllowlist...), step.allow...)

	activeOnly = s.applyAllowlist(rtActive, activeOnly, rules)
	passiveOnly = s.applyAllowlist(rtPassive, passiveOnly, rules)

	// t.Logf only surfaces under -v or on failure, so this is free in a normal run and
	// answers "what did this step actually compare?" when you need it.
	if len(activeIdentities) > 0 || len(passiveIdentities) > 0 {
		s.T().Logf("step %q %s: matched=%d %v", step.name, category.Name(), matched, activeIdentities)
	}

	if len(activeOnly) == 0 && len(passiveOnly) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "step %q: %s tasks diverge\n", step.name, category.Name())
	fmt.Fprintf(&b, "  active-only (%d):\n", len(activeOnly))
	for _, identity := range activeOnly {
		fmt.Fprintf(&b, "    %s\n", identity)
	}
	fmt.Fprintf(&b, "  passive-only (%d):\n", len(passiveOnly))
	for _, identity := range passiveOnly {
		fmt.Fprintf(&b, "    %s\n", identity)
	}
	fmt.Fprintf(&b, "  matched (%d)\n", matched)
	fmt.Fprintf(&b, "  allowlist rules consulted (%d): %s\n", len(rules), rtRuleNames(rules))
	b.WriteString("  none of them matched the divergence above.\n")
	b.WriteString("  Either this is a real bug, or add a rule with a reason and a code citation.")

	// Errorf rather than a require-style failure: a case is a sequence of steps, and one
	// divergence in step 2 should not hide what steps 3..N would have reported. One run
	// should surface every divergence in the sequence.
	s.T().Errorf("%s", b.String())
}

func (s *rtSuite) normalizeAll(in []tasks.Task) []string {
	out := make([]string, 0, len(in))
	for _, task := range in {
		if _, excluded := rtExcludedCategories[task.GetCategory()]; excluded {
			continue
		}
		identity := rtNormalize(task)
		s.NotContains(identity, "UNHANDLED",
			"add %T to rtNormalize and decide whether it belongs in the allowlist", task)
		out = append(out, identity)
	}
	sort.Strings(out)
	return out
}

// applyAllowlist removes waived identities and records which rules fired.
func (s *rtSuite) applyAllowlist(side rtSide, identities []string, rules []rtAllowRule) []string {
	var remaining []string
	for _, identity := range identities {
		waived := false
		for _, rule := range rules {
			s.NotEmpty(rule.reason, "allowlist rule %q needs a reason", rule.name)
			s.NotEmpty(rule.ref, "allowlist rule %q needs a code citation", rule.name)
			if rule.match(side, identity) {
				s.firedRules[rule.name]++
				waived = true
				break
			}
		}
		if !waived {
			remaining = append(remaining, identity)
		}
	}
	return remaining
}

// reportUnusedAllowRules prints rules that never fired. It does not fail: some rules cover
// paths a given case never reaches. But an allowlist nobody prunes is how a differential
// test quietly stops testing anything.
func (s *rtSuite) reportUnusedAllowRules() {
	var unused, defectsHit []string
	for _, rule := range rtGlobalAllowlist {
		switch {
		case s.firedRules[rule.name] == 0:
			unused = append(unused, rule.name)
		case rule.knownDefect:
			defectsHit = append(defectsHit,
				fmt.Sprintf("%s (x%d)", rule.name, s.firedRules[rule.name]))
		}
	}
	if len(unused) > 0 {
		s.T().Logf("round-trip allowlist rules that never fired: %s", strings.Join(unused, ", "))
	}
	if len(defectsHit) > 0 {
		s.T().Logf("KNOWN PASSIVE-SIDE DEFECTS still being waived: %s", strings.Join(defectsHit, ", "))
	}
}

func rtDiffMultisets(active, passive []string) (activeOnly, passiveOnly []string, matched int) {
	counts := make(map[string]int)
	for _, identity := range active {
		counts[identity]++
	}
	for _, identity := range passive {
		if counts[identity] > 0 {
			counts[identity]--
			matched++
		} else {
			passiveOnly = append(passiveOnly, identity)
		}
	}
	for identity, remaining := range counts {
		for i := 0; i < remaining; i++ {
			activeOnly = append(activeOnly, identity)
		}
	}
	sort.Strings(activeOnly)
	sort.Strings(passiveOnly)
	return activeOnly, passiveOnly, matched
}

func rtRuleNames(rules []rtAllowRule) string {
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.name)
	}
	return strings.Join(names, ", ")
}
