package versionguard

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/service/worker/scheduler"
)

// Gate is one SchedulerWorkflowVersion's frozen-gate proof. IsObserved reports whether the
// gate's behavior is present in a recorded history. The generator records each gate's minimal
// scenario twice (unclamped, where IsObserved must be true, and clamped at Version-1, where
// it must be false) and commits both recorded histories; TestVersionGates re-asserts that
// on/off split on every run, and TestReplays replays both so an ungated behavior change breaks
// the frozen history it reaches.
type Gate struct {
	Version    scheduler.SchedulerWorkflowVersion
	Name       string
	Why        string // one line: the behavior this gate introduced
	IsObserved func(*Observations) bool
}

// CustomSearchAttribute is the pre-registered search attribute the search-attribute-upsert
// gate's scenario carries.
const CustomSearchAttribute = "CustomKeywordField"

// GateFixtureName returns the recorded-history file name for a gate. current=true is the
// unclamped recording at the current version (gate present); false is the recording clamped
// just below the gate version (gate absent). The version prefix sorts the corpus by gate and
// disambiguates the two gates that share a version; the replay_ prefix puts both in
// TestReplays' glob.
func GateFixtureName(version int, name string, current bool) string {
	side := "clamped"
	if current {
		side = "current"
	}
	return fmt.Sprintf("replay_gate_v%02d_%s_%s.json.gz", version, name, side)
}

// Gates lists every version gate with a reachable, history-observable on/off signal, one
// per behavior. Gates without such a signal are covered by replay alone, not on/off:
//   - v1 BatchAndCacheTimeQueries, v2 NewCacheAndJitter: below the practical clamp floor
//     (v1.23.1 records version 3; ceiling 0 means "unset", so "below v1/v2" is unreachable).
//   - v6 UpdateFromPrevious reprocessing: no deterministic signal on jitter-free specs.
//   - v7 CANAfterSignals: control-flow timing, no command-level signal.
//
// These are frozen by TestReplays over the pre-existing testdata/replay_* fixtures
// (replay_with_proto_cache, replay_with_use_last_action, replay_v1.19.1 ... replay_v1.23-pre).
func Gates() []Gate {
	limit := scheduler.CurrentTweakablePolicies.SpecFieldLengthLimit
	return []Gate{
		{
			// v3+ stops tracking ALLOW_ALL starts in Info.RunningWorkflows; below v3 they were
			// listed. Present iff both ALLOW_ALL actions fired but RunningWorkflows stays empty.
			Version: scheduler.DontTrackOverlapping,
			Name:    "allow-all-untracked",
			Why:     "ALLOW_ALL starts are not tracked in Info.RunningWorkflows",
			IsObserved: func(o *Observations) bool {
				return o.CANInput != nil && o.AllowAllTriggers() >= 2 && len(o.RecentActions) >= 2 &&
					len(o.CANInput.GetInfo().GetRunningWorkflows()) == 0
			},
		},
		{
			// v4+ treats a backfill's start time as inclusive, so a single-point backfill on the
			// grid fires; below v4 the exclusive start skipped it.
			Version: scheduler.InclusiveBackfillStartTime,
			Name:    "inclusive-backfill",
			Why:     "a single-point backfill on the grid fires (start time is inclusive)",
			IsObserved: func(o *Observations) bool {
				point, ok := o.PointBackfill()
				return ok && o.HasActionAt(point)
			},
		},
		{
			// v5+ carries an un-drained backfill range forward on the continue-as-new input as
			// OngoingBackfills; below v5 the whole range was buffered synchronously.
			Version: scheduler.IncrementalBackfill,
			Name:    "incremental-backfill",
			Why:     "a wide backfill range rides the continue-as-new input as OngoingBackfills",
			IsObserved: func(o *Observations) bool {
				return o.CANInput != nil && len(o.CANInput.GetState().GetOngoingBackfills()) > 0
			},
		},
		{
			// v6+ serializes the next-time cache as proto; v2..v5 used JSON.
			Version: scheduler.UpdateFromPrevious,
			Name:    "proto-cache",
			Why:     "the next-time cache is serialized as proto rather than JSON",
			IsObserved: func(o *Observations) bool {
				return o.ProtoCacheMarkers > 0 && o.JSONCacheMarkers == 0
			},
		},
		{
			// v6+ upserts the schedule's custom search attributes on update; below v6 the upsert
			// was skipped.
			Version: scheduler.UpdateFromPrevious,
			Name:    "search-attribute-upsert",
			Why:     "an update upserts the schedule's custom search attributes",
			IsObserved: func(o *Observations) bool {
				return o.SAUpserts[CustomSearchAttribute] > 0
			},
		},
		{
			// v8+ records LastProcessedTime as the last action's nominal time; below v8 it was the
			// wall-clock wakeup, which is later.
			Version: scheduler.UseLastAction,
			Name:    "last-action-time",
			Why:     "LastProcessedTime is the last action's time, not the wall-clock wakeup",
			IsObserved: func(o *Observations) bool {
				if o.CANInput == nil || len(o.RecentActions) == 0 {
					return false
				}
				return o.CANInput.GetState().GetLastProcessedTime().AsTime().Equal(o.MaxActionNominal())
			},
		},
		{
			// v9+ caps a LimitedActions schedule's advertised future times to RemainingActions
			// (one here); below v9 it advertised the full FutureActionCountForList list.
			Version: scheduler.AccurateFutureActionTimes,
			Name:    "future-times-limited",
			Why:     "future action times honor RemainingActions (capped to the one remaining)",
			IsObserved: func(o *Observations) bool {
				return o.MaxMemoSpecEntries > 0 && o.MaxFutureActionTimes == 1
			},
		},
		{
			// v10+ records a workflow status on each action (RUNNING at start); below v10 it
			// stayed UNSPECIFIED.
			Version: scheduler.ActionResultIncludesStatus,
			Name:    "action-status",
			Why:     "a recorded action carries a non-UNSPECIFIED workflow status",
			IsObserved: func(o *Observations) bool {
				for _, status := range o.RecentActions {
					if status != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
						return true
					}
				}
				return false
			},
		},
		{
			// v11+ trims an oversized spec to SpecFieldLengthLimit entries in the memo (the stored
			// schedule keeps the full spec); below v11 the memo carried the full spec.
			Version: scheduler.LimitMemoSpecSize,
			Name:    "memo-spec-trimmed",
			Why:     "an oversized spec is trimmed to SpecFieldLengthLimit entries in the memo",
			IsObserved: func(o *Observations) bool {
				return o.SpecEntries() > limit && o.MaxMemoSpecEntries > 0 && o.MaxMemoSpecEntries <= limit
			},
		},
		{
			// v12+ fires a trigger at the ScheduledTime supplied in the patch; below v12 it fired
			// at the signal-processing time.
			Version: scheduler.TriggerImmediatelyTimestamp,
			Name:    "trigger-supplied-time",
			Why:     "a trigger fires with the ScheduledTime supplied in the patch",
			IsObserved: func(o *Observations) bool {
				supplied, ok := o.SuppliedTriggerTime()
				return ok && o.HasActionAt(supplied)
			},
		},
	}
}
