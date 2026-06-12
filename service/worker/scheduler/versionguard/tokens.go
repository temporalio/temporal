package versionguard

import (
	"errors"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/service/worker/scheduler"
)

// TokenState is the three-valued outcome of a token detector. NotExercised is a loud
// failure in every context: it means the scenario no longer trips the gate the token
// exists to freeze, so the snapshot proves nothing about that version.
type TokenState int

const (
	NotExercised TokenState = iota
	Off                     // the pre-version behavior manifested
	On                      // the version's behavior manifested
)

func (s TokenState) String() string {
	switch s {
	case On:
		return "On"
	case Off:
		return "Off"
	default:
		return "NotExercised"
	}
}

// Token is the history-observable proof for one SchedulerWorkflowVersion. Detect
// reports which side of the version's gate a snapshot's recorded decision procedure took:
// On when the version is in force, Off when the recorded version is clamped below it. A
// snapshot clamped to N must show On for every token at or below N and Off above it (and
// never NotExercised).
//
// Every token is documented in the same fixed shape. Stimulus/WhenOn/WhenOff/Note describe
// the gate uniformly and are printed by Evaluate when a token fails, so a broken gate
// reports exactly what it expected to see.
type Token struct {
	Version  scheduler.SchedulerWorkflowVersion
	Name     string
	Scenario string

	// Stimulus is what the scenario does to exercise this version's gate.
	Stimulus string
	// WhenOn is the history evidence observed when the version is in force (recorded version >= Version).
	WhenOn string
	// WhenOff is the history evidence observed when the recorded version is clamped below the gate.
	WhenOff string
	// Note is an optional caveat (an unobservable side, a non-default allowed set, ...); "" if none.
	Note string

	Detect func(*Artifacts) TokenState
	// allowed overrides the default expectation (On at or below the clamp, Off above, never NotExercised) for
	// versions whose artifact is not deterministically observable on both sides; nil means
	// the default applies.
	allowed func(clamp int) []TokenState
}

// allowedStates returns the acceptable detector outcomes for this token at the clamp.
func (t Token) allowedStates(clamp int) []TokenState {
	if t.allowed != nil {
		return t.allowed(clamp)
	}
	if int(t.Version) <= clamp {
		return []TokenState{On}
	}
	return []TokenState{Off}
}

// expectation renders the token's fixed-shape documentation as an indented block, appended
// to the Evaluate failure message so a broken gate states what it expected on each side.
func (t Token) expectation() string {
	b := fmt.Sprintf("\n    stimulus: %s\n    on:       %s\n    off:      %s", t.Stimulus, t.WhenOn, t.WhenOff)
	if t.Note != "" {
		b += "\n    note:     " + t.Note
	}
	return b
}

// Scenario names. Each scenario is one schedule lifecycle the generator drives; a token
// belongs to exactly one scenario, chosen so its stimulus does not disturb the others.
const (
	// ScenarioCore: an unpaused schedule on a coarse grid, driven through a timestamped
	// trigger, a single-point backfill, a spec update (to a 5-second grid) carrying a
	// custom search attribute, a large range backfill, and force-continue-as-new.
	ScenarioCore = "core"
	// ScenarioLimited: an unpaused LimitedActions schedule with an oversized spec on a
	// coarse grid and a long-running action, driven through two ALLOW_ALL triggers and
	// force-continue-as-new.
	ScenarioLimited = "limited"
)

// CustomSearchAttribute is the pre-registered search attribute the core scenario's update
// carries; the v8 token detects its upsert.
const CustomSearchAttribute = "CustomKeywordField"

// Scenarios lists every scenario a snapshot must exist for, per version.
func Scenarios() []string { return []string{ScenarioCore, ScenarioLimited} }

// MaxVersion is the highest activated version, and therefore the highest clamp a snapshot
// can be generated for. Defined-but-unactivated constants get their snapshots in the
// release that activates them (the clamp can only lower the recorded version).
func MaxVersion() int { return int(scheduler.CurrentTweakablePolicies.Version) }

// SnapshotName returns the testdata file name for a (version, scenario) snapshot. The
// replay_ prefix puts snapshots in TestReplays' glob, so every snapshot is also replayed
// by the current binary on every run.
func SnapshotName(version int, scenario string) string {
	return fmt.Sprintf("replay_snapshot_v%02d_%s.json.gz", version, scenario)
}

// Tokens returns the per-version tokens, one per activated version, in version
// order. Each token is built by its own constructor below so every detector stays
// independently readable; add the next version's constructor here when you add it.
func Tokens() []Token {
	return []Token{
		tokenTimeQueriesCachedV1(),
		tokenCacheV2(),
		tokenAllowAllUntrackedV3(),
		tokenBackfillStartInclusiveV4(),
		tokenBackfillIncrementalV5(),
		tokenUpdateReprocessesPreviousV6(),
		tokenCacheWireFormatV7(),
		tokenLastActionAndSAUpdateV8(),
		tokenFutureTimesLimitedV9(),
		tokenActionStatusRecordedV10(),
		tokenMemoSpecTrimmedV11(),
		tokenTriggerUsesSuppliedTimeV12(),
	}
}

// tokenTimeQueriesCachedV1 freezes BatchAndCacheTimeQueries (v1): GetNextTime queries are
// batched through a recorded next-time cache. This is also the PR that introduced the
// SchedulerWorkflowVersion mechanism itself.
// Introduced by https://github.com/temporalio/temporal/pull/4215, first released in v1.21.0.
func tokenTimeQueriesCachedV1() Token {
	return Token{
		Version:  scheduler.BatchAndCacheTimeQueries,
		Name:     "time-queries-cached",
		Scenario: ScenarioCore,
		Stimulus: "any firing schedule, which must compute its next times",
		WhenOn:   "a recorded time-query cache marker (V1 batch, JSON V2, or proto V2)",
		WhenOff:  "unobservable: version 0 is below the lowest version any ceiling can clamp to",
		Note:     "the Off side cannot be reached, so this token only proves the cache path ran (On or NotExercised)",
		Detect: func(a *Artifacts) TokenState {
			if a.ProtoCacheMarkers+a.JSONCacheMarkers+a.V1BatchTimeQueries > 0 {
				return On
			}
			return NotExercised
		},
	}
}

// tokenCacheV2 freezes NewCacheAndJitter (v2): time queries use the V2 cache and jitter
// includes the workflow id.
// Introduced by https://github.com/temporalio/temporal/pull/4685, first released in v1.22.0.
func tokenCacheV2() Token {
	return Token{
		Version:  scheduler.NewCacheAndJitter,
		Name:     "cache-v2",
		Scenario: ScenarioCore,
		Stimulus: "any firing schedule",
		WhenOn:   "a V2 cache marker (proto or JSON) and no V1 batch marker",
		WhenOff:  "a V1 batch marker and no V2 cache marker",
		Detect: func(a *Artifacts) TokenState {
			v2 := a.ProtoCacheMarkers + a.JSONCacheMarkers
			switch {
			case v2 > 0 && a.V1BatchTimeQueries == 0:
				return On
			case v2 == 0 && a.V1BatchTimeQueries > 0:
				return Off
			default:
				return NotExercised
			}
		},
	}
}

// tokenAllowAllUntrackedV3 freezes DontTrackOverlapping (v3): ALLOW_ALL starts are no longer
// added to Info.RunningWorkflows.
// Introduced by https://github.com/temporalio/temporal/pull/4911, first released in v1.23.0.
func tokenAllowAllUntrackedV3() Token {
	return Token{
		Version:  scheduler.DontTrackOverlapping,
		Name:     "allow-all-untracked",
		Scenario: ScenarioLimited,
		Stimulus: "two ALLOW_ALL triggers starting long-running actions, then continue-as-new",
		WhenOn:   "the continue-as-new input's Info.RunningWorkflows is empty",
		WhenOff:  "the started actions appear in Info.RunningWorkflows",
		Detect: func(a *Artifacts) TokenState {
			allowAll := 0
			for _, p := range a.Patches {
				if t := p.Patch.GetTriggerImmediately(); t != nil &&
					t.GetOverlapPolicy() == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
					allowAll++
				}
			}
			if allowAll < 2 || len(a.RecentActions) < 2 || a.CANInput == nil {
				return NotExercised
			}
			if len(a.CANInput.GetInfo().GetRunningWorkflows()) == 0 {
				return On
			}
			return Off
		},
	}
}

// tokenBackfillStartInclusiveV4 freezes InclusiveBackfillStartTime (v4): a backfill's start
// time is inclusive rather than exclusive.
// Introduced by https://github.com/temporalio/temporal/pull/5179, first released in v1.23.0.
func tokenBackfillStartInclusiveV4() Token {
	return Token{
		Version:  scheduler.InclusiveBackfillStartTime,
		Name:     "backfill-start-inclusive",
		Scenario: ScenarioCore,
		Stimulus: "a single-point backfill (StartTime == EndTime) exactly on the spec grid",
		WhenOn:   "the point fires (HasActionAt)",
		WhenOff:  "nothing fires; the start time is treated as exclusive",
		Detect: func(a *Artifacts) TokenState {
			point, ok := pointBackfill(a)
			if !ok || len(a.RecentActions) == 0 {
				return NotExercised
			}
			if a.HasActionAt(point) {
				return On
			}
			return Off
		},
	}
}

// tokenBackfillIncrementalV5 freezes IncrementalBackfill (v5): a backfill range is processed
// incrementally across iterations instead of synchronously in one pass.
// Introduced by https://github.com/temporalio/temporal/pull/5344, first released in v1.24.0.
func tokenBackfillIncrementalV5() Token {
	return Token{
		Version:  scheduler.IncrementalBackfill,
		Name:     "backfill-incremental",
		Scenario: ScenarioCore,
		Stimulus: "a large range backfill, then immediate force-continue-as-new",
		WhenOn:   "the remainder rides the continue-as-new input as OngoingBackfills",
		WhenOff:  "the whole range was buffered synchronously, riding as BufferedStarts",
		Detect: func(a *Artifacts) TokenState {
			if _, ok := rangeBackfill(a); !ok || a.CANInput == nil {
				return NotExercised
			}
			if len(a.CANInput.GetState().GetOngoingBackfills()) > 0 {
				return On
			}
			if len(a.CANInput.GetState().GetBufferedStarts()) >= 5 {
				return Off
			}
			return NotExercised
		},
	}
}

// tokenUpdateReprocessesPreviousV6 freezes UpdateFromPrevious (v6): after an update the
// window before it is reprocessed so jittered actions whose nominal time predates the update
// but whose actual time follows it still fire.
// Introduced by https://github.com/temporalio/temporal/pull/5381, first released in v1.24.0.
func tokenUpdateReprocessesPreviousV6() Token {
	return Token{
		Version:  scheduler.UpdateFromPrevious,
		Name:     "update-reprocesses-previous",
		Scenario: ScenarioCore,
		Stimulus: "a spec update to a fine (5-second) grid following a pre-update fire",
		WhenOn:   "a fire on the new grid in the pre-update window",
		WhenOff:  "no pre-update-window fire on the new grid",
		Note: "processTimeRange discards plain past nominals against UpdateTime, so on jitter-free specs v5 and v6+ emit " +
			"identical commands and On has no deterministic artifact; allowed therefore requires Off below 6 and accepts " +
			"{Off,On} at 6+. The v6 code path is still frozen by replaying the v06/v07 snapshots. Detection ignores :00 " +
			"seconds so coarse-grid fires cannot alias.",
		Detect: func(a *Artifacts) TokenState {
			updateAt, ok := updateTime(a)
			if !ok {
				return NotExercised
			}
			if hasFineGridActionIn(a, updateAt.Add(-30*time.Second), updateAt.Add(-2*time.Second)) {
				return On
			}
			// The new grid demonstrably took effect after the update.
			if hasFineGridActionIn(a, updateAt, updateAt.Add(30*time.Second)) {
				return Off
			}
			return NotExercised
		},
		allowed: func(clamp int) []TokenState {
			if clamp < int(scheduler.UpdateFromPrevious) {
				return []TokenState{Off}
			}
			return []TokenState{Off, On}
		},
	}
}

// tokenCacheWireFormatV7 freezes CANAfterSignals (v7): the V2 next-time cache is serialized
// as proto rather than JSON. This is the encoding pre-1.23.1 readers cannot decode.
// CANAfterSignals introduced by https://github.com/temporalio/temporal/pull/5799, first
// released in v1.24.0; this PR gates the cache wire format on the recorded version.
func tokenCacheWireFormatV7() Token {
	return Token{
		Version:  scheduler.CANAfterSignals,
		Name:     "cache-wire-format",
		Scenario: ScenarioCore,
		Stimulus: "any firing schedule (the cache is filled every run)",
		WhenOn:   "a proto next-time-cache marker and no JSON cache marker",
		WhenOff:  "a JSON cache marker (or, below v2, a V1 batch marker)",
		Detect: func(a *Artifacts) TokenState {
			switch {
			case a.ProtoCacheMarkers > 0 && a.JSONCacheMarkers == 0:
				return On
			case a.ProtoCacheMarkers == 0 && (a.JSONCacheMarkers > 0 || a.V1BatchTimeQueries > 0):
				return Off
			default:
				return NotExercised
			}
		},
	}
}

// tokenLastActionAndSAUpdateV8 freezes UseLastAction (v8): LastProcessedTime is set to the
// last action's nominal time (not the wall-clock wakeup), and an update upserts custom
// search attributes.
// Introduced by https://github.com/temporalio/temporal/pull/6028, first released in v1.25.0.
func tokenLastActionAndSAUpdateV8() Token {
	return Token{
		Version:  scheduler.UseLastAction,
		Name:     "last-action-time-and-sa-update",
		Scenario: ScenarioCore,
		Stimulus: "an update carrying a custom search attribute, after a scheduled fire",
		WhenOn:   "the search attribute is upserted and the CAN input's LastProcessedTime equals the last action's nominal time",
		WhenOff:  "no search-attribute upsert; LastProcessedTime is the wall-clock wakeup, later than every nominal",
		Note:     "two behaviors ship together at v8, so the detector requires both sides to agree before reporting On or Off",
		Detect: func(a *Artifacts) TokenState {
			if !updateHasSA(a) || a.CANInput == nil || len(a.RecentActions) == 0 {
				return NotExercised
			}
			lp := a.CANInput.GetState().GetLastProcessedTime().AsTime()
			saOn := a.SAUpserts[CustomSearchAttribute] > 0
			lastActionOn := lp.Equal(a.MaxActionNominal())
			switch {
			case saOn && lastActionOn:
				return On
			case !saOn && lp.After(a.MaxActionNominal()):
				return Off
			default:
				return NotExercised
			}
		},
	}
}

// tokenFutureTimesLimitedV9 freezes AccurateFutureActionTimes (v9): the memo's future action
// times honor RemainingActions and the update time.
// Introduced by https://github.com/temporalio/temporal/pull/6122, first released in v1.25.0.
func tokenFutureTimesLimitedV9() Token {
	return Token{
		Version:  scheduler.AccurateFutureActionTimes,
		Name:     "future-times-limited",
		Scenario: ScenarioLimited,
		Stimulus: "a LimitedActions schedule with a single remaining action",
		WhenOn:   "the memo advertises at most one future action time",
		WhenOff:  "the memo advertises the full FutureActionCountForList list",
		Detect: func(a *Artifacts) TokenState {
			if a.CANInput == nil || !a.CANInput.GetSchedule().GetState().GetLimitedActions() {
				return NotExercised
			}
			forList := scheduler.CurrentTweakablePolicies.FutureActionCountForList
			switch {
			case a.MaxFutureActionTimes >= forList:
				return Off
			case a.MaxFutureActionTimes <= 1 && a.MaxMemoSpecEntries > 0:
				return On
			default:
				return NotExercised
			}
		},
	}
}

// tokenActionStatusRecordedV10 freezes ActionResultIncludesStatus (v10): recorded actions
// carry a WorkflowExecutionStatus.
// Introduced by https://github.com/temporalio/temporal/pull/6665, first released in v1.26.2.
func tokenActionStatusRecordedV10() Token {
	return Token{
		Version:  scheduler.ActionResultIncludesStatus,
		Name:     "action-status-recorded",
		Scenario: ScenarioCore,
		Stimulus: "any schedule that records an action result",
		WhenOn:   "at least one recorded action carries a non-UNSPECIFIED status",
		WhenOff:  "every recorded action's status is UNSPECIFIED",
		Detect: func(a *Artifacts) TokenState {
			if len(a.RecentActions) == 0 {
				return NotExercised
			}
			for _, status := range a.RecentActions {
				if status != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
					return On
				}
			}
			return Off
		},
	}
}

// tokenMemoSpecTrimmedV11 freezes LimitMemoSpecSize (v11): the visibility memo's spec fields
// are trimmed to SpecFieldLengthLimit entries.
// Introduced by https://github.com/temporalio/temporal/pull/7356, first released in v1.28.0.
func tokenMemoSpecTrimmedV11() Token {
	return Token{
		Version:  scheduler.LimitMemoSpecSize,
		Name:     "memo-spec-trimmed",
		Scenario: ScenarioLimited,
		Stimulus: "a schedule whose spec exceeds SpecFieldLengthLimit entries",
		WhenOn:   "the memo's spec is trimmed to SpecFieldLengthLimit entries",
		WhenOff:  "the memo carries the full, untrimmed spec",
		Detect: func(a *Artifacts) TokenState {
			limit := scheduler.CurrentTweakablePolicies.SpecFieldLengthLimit
			if specEntries(a) <= limit || a.MaxMemoSpecEntries == 0 {
				return NotExercised
			}
			if a.MaxMemoSpecEntries <= limit {
				return On
			}
			return Off
		},
	}
}

// tokenTriggerUsesSuppliedTimeV12 freezes TriggerImmediatelyTimestamp (v12): a trigger fires
// with the ScheduledTime supplied in the patch rather than the signal-processing time.
// Introduced by https://github.com/temporalio/temporal/pull/7968, first released in v1.29.0.
func tokenTriggerUsesSuppliedTimeV12() Token {
	return Token{
		Version:  scheduler.TriggerImmediatelyTimestamp,
		Name:     "trigger-uses-supplied-time",
		Scenario: ScenarioCore,
		Stimulus: "a trigger carrying an explicit off-grid ScheduledTime",
		WhenOn:   "a fire at the supplied nominal time (HasActionAt)",
		WhenOff:  "a fire at the signal-processing time instead",
		Detect: func(a *Artifacts) TokenState {
			supplied, signalAt, ok := timestampedTrigger(a)
			if !ok {
				return NotExercised
			}
			if a.HasActionAt(supplied) {
				return On
			}
			if a.HasActionIn(signalAt.Add(-2*time.Second), signalAt.Add(6*time.Second)) {
				return Off
			}
			return NotExercised
		},
	}
}

// TokensForScenario filters the table to one scenario.
func TokensForScenario(scenario string) []Token {
	var out []Token
	for _, t := range Tokens() {
		if t.Scenario == scenario {
			out = append(out, t)
		}
	}
	return out
}

// Evaluate asserts the full token matrix for one snapshot: the recorded tweakables
// markers carry exactly the clamp (in the JSON wire shape old binaries decode), and every
// token owned by the scenario reports an allowed state for the clamp, never NotExercised.
// The returned error joins every violation.
func Evaluate(events []*historypb.HistoryEvent, clamp int, scenario string) error {
	a, err := Parse(events)
	if err != nil {
		return err
	}
	var errs []error
	if len(a.TweakableVersions) == 0 {
		errs = append(errs, errors.New("no tweakables markers recorded"))
	}
	for i, v := range a.TweakableVersions {
		if v != int64(clamp) {
			errs = append(errs, fmt.Errorf("tweakables marker %d records version %d, want clamp %d", i, v, clamp))
		}
		if a.TweakableEncodings[i] != encodingJSON {
			errs = append(errs, fmt.Errorf("tweakables marker %d encoded as %q, want %q", i, a.TweakableEncodings[i], encodingJSON))
		}
	}
	for _, token := range TokensForScenario(scenario) {
		allowed := token.allowedStates(clamp)
		if got := token.Detect(a); !slices.Contains(allowed, got) {
			errs = append(errs, fmt.Errorf("token v%d %q: got %s, want %v%s",
				token.Version, token.Name, got, allowed, token.expectation()))
		}
	}
	return errors.Join(errs...)
}

// Detector helpers, each deriving its reference points from the history itself so
// detectors need no out-of-band scenario constants.

func pointBackfill(a *Artifacts) (time.Time, bool) {
	for _, p := range a.Patches {
		for _, b := range p.Patch.GetBackfillRequest() {
			start, end := b.GetStartTime().AsTime(), b.GetEndTime().AsTime()
			if start.Equal(end) {
				return start.Truncate(time.Second), true
			}
		}
	}
	return time.Time{}, false
}

func rangeBackfill(a *Artifacts) (time.Time, bool) {
	for _, p := range a.Patches {
		for _, b := range p.Patch.GetBackfillRequest() {
			start, end := b.GetStartTime().AsTime(), b.GetEndTime().AsTime()
			if end.After(start) {
				return start, true
			}
		}
	}
	return time.Time{}, false
}

func updateTime(a *Artifacts) (time.Time, bool) {
	if len(a.Updates) == 0 {
		return time.Time{}, false
	}
	return a.Updates[0].Time, true
}

func updateHasSA(a *Artifacts) bool {
	for _, u := range a.Updates {
		if _, ok := u.Update.GetSearchAttributes().GetIndexedFields()[CustomSearchAttribute]; ok {
			return true
		}
	}
	return false
}

// hasFineGridActionIn reports a fire on the update's 5-second grid inside [from, to),
// excluding :00 seconds so fires from coarse (minute-aligned) grids cannot alias.
func hasFineGridActionIn(a *Artifacts, from, to time.Time) bool {
	for nominal := range a.RecentActions {
		if nominal.Before(from) || !nominal.Before(to) {
			continue
		}
		if sec := nominal.Second(); sec%5 == 0 && sec != 0 {
			return true
		}
	}
	return false
}

func timestampedTrigger(a *Artifacts) (supplied time.Time, signalAt time.Time, ok bool) {
	for _, p := range a.Patches {
		if t := p.Patch.GetTriggerImmediately(); t != nil && t.GetScheduledTime() != nil {
			return t.GetScheduledTime().AsTime().Truncate(time.Second), p.Time, true
		}
	}
	return time.Time{}, time.Time{}, false
}

func specEntries(a *Artifacts) int {
	spec := a.CANInput.GetSchedule().GetSpec()
	return max(len(spec.GetInterval()), len(spec.GetStructuredCalendar()), len(spec.GetExcludeStructuredCalendar()))
}
