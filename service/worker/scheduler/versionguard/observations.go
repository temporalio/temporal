// Package versionguard freezes the V1 scheduler's per-version decision procedure.
//
// Each SchedulerWorkflowVersion gates behavior that must never change once shipped (the
// frozen-gate invariant the worker.schedulerVersionCeiling dynamic config relies on). Gates
// (see gates.go) lists every gate with a minimal scenario and an IsObserved predicate over a
// recorded history. The generator records each gate's scenario twice, unclamped (the current
// version) and clamped just below the gate version, and commits both recorded histories under
// service/worker/scheduler/testdata. Two guards then hold forever:
//
//   - On/off: TestVersionGates asserts each gate is observed in the unclamped recording and
//     absent in the clamped one, so a misgated or broken clamp is caught.
//   - Replay: TestReplays replays every recorded history under the current binary, so an
//     ungated behavior change breaks the frozen history it reaches.
//
// Adding version N+1: add a Gate entry to gates.go, add its minimal scenario to the
// generator, and generate the new history pair. Existing histories must never be regenerated
// (deleting one is a deliberate, review-visible act).
package versionguard

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/converter"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/worker/scheduler"
)

// Observations is the single-pass projection of a scheduler workflow history into the facts the
// gates' IsObserved predicates read, so each predicate stays a few lines and new gates compose
// existing facts.
type Observations struct {
	// TweakableVersions holds the Version recorded by each "tweakables" MutableSideEffect
	// marker, in history order. TweakableEncodings holds the matching payload encodings.
	TweakableVersions  []int64
	TweakableEncodings []string

	// Next-time cache SideEffect markers by wire format.
	ProtoCacheMarkers int
	JSONCacheMarkers  int

	// RecentActions is the union of ScheduleActionResult entries observed across every
	// memo snapshot (the start event's initial memo plus each memo rewrite), keyed by
	// nominal schedule time truncated to seconds.
	RecentActions map[time.Time]enumspb.WorkflowExecutionStatus

	// MaxFutureActionTimes is the largest FutureActionTimes list seen in any memo
	// snapshot; MaxMemoSpecEntries is the largest per-field spec entry count seen.
	MaxFutureActionTimes int
	MaxMemoSpecEntries   int

	// SAUpserts counts UpsertWorkflowSearchAttributes events per attribute key.
	SAUpserts map[string]int

	// Patches are the schedule patch signals received, with event timestamps.
	Patches []TimedPatch

	// CANInput is the StartScheduleArgs carried by the run's continue-as-new event, nil if
	// the run did not continue-as-new.
	CANInput *schedulespb.StartScheduleArgs
}

type TimedPatch struct {
	Time  time.Time
	Patch *schedulepb.SchedulePatch
}

const (
	markerSideEffect        = "SideEffect"
	markerMutableSideEffect = "MutableSideEffect"
	memoFieldScheduleInfo   = "ScheduleInfo"
	protoCacheMessageType   = "temporal.server.api.schedule.v1.NextTimeCache"
	encodingJSON            = "json/plain"
)

// Observe projects a scheduler workflow history into Observations.
func Observe(events []*historypb.HistoryEvent) (*Observations, error) {
	o := &Observations{
		RecentActions: map[time.Time]enumspb.WorkflowExecutionStatus{},
		SAUpserts:     map[string]int{},
	}
	dc := converter.GetDefaultDataConverter()
	for _, event := range events {
		switch {
		case event.GetMarkerRecordedEventAttributes() != nil:
			if err := o.parseMarker(dc, event.GetMarkerRecordedEventAttributes()); err != nil {
				return nil, fmt.Errorf("event %d: %w", event.GetEventId(), err)
			}
		case event.GetWorkflowExecutionStartedEventAttributes() != nil:
			o.parseMemo(event.GetWorkflowExecutionStartedEventAttributes().GetMemo())
		case event.GetWorkflowPropertiesModifiedEventAttributes() != nil:
			o.parseMemo(event.GetWorkflowPropertiesModifiedEventAttributes().GetUpsertedMemo())
		case event.GetUpsertWorkflowSearchAttributesEventAttributes() != nil:
			for key := range event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields() {
				o.SAUpserts[key]++
			}
		case event.GetWorkflowExecutionSignaledEventAttributes() != nil:
			if err := o.parseSignal(event); err != nil {
				return nil, fmt.Errorf("event %d: %w", event.GetEventId(), err)
			}
		case event.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
			var args schedulespb.StartScheduleArgs
			if err := payloads.Decode(event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetInput(), &args); err != nil {
				return nil, fmt.Errorf("event %d: decoding continue-as-new input: %w", event.GetEventId(), err)
			}
			o.CANInput = &args
		default:
			// Other event types carry no version observations.
		}
	}
	return o, nil
}

func (o *Observations) parseMarker(dc converter.DataConverter, marker *historypb.MarkerRecordedEventAttributes) error {
	switch marker.GetMarkerName() {
	case markerMutableSideEffect:
		id, ok := sideEffectID(dc, marker)
		if !ok || !strings.HasPrefix(id, "tweakables") {
			return nil
		}
		// MutableSideEffect data holds [side-effect id, wrapped Payloads of the value].
		data := marker.GetDetails()["data"]
		if data == nil || len(data.Payloads) < 2 {
			return errors.New("tweakables marker has malformed data details")
		}
		var wrapped commonpb.Payloads
		if err := dc.FromPayload(data.Payloads[1], &wrapped); err != nil {
			return fmt.Errorf("unwrapping tweakables marker: %w", err)
		}
		if len(wrapped.Payloads) == 0 {
			return errors.New("tweakables marker has no value payload")
		}
		value := wrapped.Payloads[0]
		var policies struct{ Version int64 }
		if err := dc.FromPayload(value, &policies); err != nil {
			return fmt.Errorf("decoding tweakables value: %w", err)
		}
		o.TweakableVersions = append(o.TweakableVersions, policies.Version)
		o.TweakableEncodings = append(o.TweakableEncodings, string(value.Metadata["encoding"]))
	case markerSideEffect:
		data := marker.GetDetails()["data"]
		if data == nil || len(data.Payloads) == 0 {
			return nil
		}
		o.classifySideEffect(data.Payloads[0])
	default:
		// Other markers (local activities, versions) carry no version observations.
	}
	return nil
}

// classifySideEffect buckets a plain SideEffect payload into the next-time-cache wire formats
// the scheduler has used across versions. Payloads that match none (e.g. the UUID batch) are
// ignored.
func (o *Observations) classifySideEffect(p *commonpb.Payload) {
	if string(p.Metadata["messageType"]) == protoCacheMessageType {
		o.ProtoCacheMarkers++
		return
	}
	if string(p.Metadata["encoding"]) != encodingJSON {
		return
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(p.Data, &obj) != nil {
		return
	}
	// The JSON V2 cache (jsonNextTimeCacheV2) carries Start and Results fields.
	if _, hasStart := obj["Start"]; hasStart {
		if _, hasResults := obj["Results"]; hasResults {
			o.JSONCacheMarkers++
			return
		}
	}
}

func (o *Observations) parseMemo(memo *commonpb.Memo) {
	p := memo.GetFields()[memoFieldScheduleInfo]
	if p == nil {
		return
	}
	var infoBytes []byte
	if converter.GetDefaultDataConverter().FromPayload(p, &infoBytes) != nil {
		return
	}
	var info schedulepb.ScheduleListInfo
	if info.Unmarshal(infoBytes) != nil {
		return
	}
	for _, action := range info.RecentActions {
		nominal := action.GetScheduleTime().AsTime().Truncate(time.Second)
		// Statuses can progress (e.g. RUNNING then COMPLETED); keep the latest non-zero.
		if action.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED ||
			o.RecentActions[nominal] == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			o.RecentActions[nominal] = action.GetStartWorkflowStatus()
		}
	}
	o.MaxFutureActionTimes = max(o.MaxFutureActionTimes, len(info.FutureActionTimes))
	spec := info.GetSpec()
	o.MaxMemoSpecEntries = max(o.MaxMemoSpecEntries,
		len(spec.GetInterval()), len(spec.GetStructuredCalendar()), len(spec.GetExcludeStructuredCalendar()))
}

func (o *Observations) parseSignal(event *historypb.HistoryEvent) error {
	attrs := event.GetWorkflowExecutionSignaledEventAttributes()
	at := event.GetEventTime().AsTime()
	switch attrs.GetSignalName() {
	case scheduler.SignalNamePatch:
		var patch schedulepb.SchedulePatch
		if err := payloads.Decode(attrs.GetInput(), &patch); err != nil {
			return fmt.Errorf("decoding patch signal: %w", err)
		}
		o.Patches = append(o.Patches, TimedPatch{Time: at, Patch: &patch})
	default:
		// Other signals (refresh, force-CAN, migrate) carry no decoded payloads we need.
	}
	return nil
}

func sideEffectID(dc converter.DataConverter, marker *historypb.MarkerRecordedEventAttributes) (string, bool) {
	ids := marker.GetDetails()["side-effect-id"]
	if ids == nil || len(ids.Payloads) == 0 {
		return "", false
	}
	var id string
	if dc.FromPayload(ids.Payloads[0], &id) != nil {
		return "", false
	}
	return id, true
}

// HasActionAt reports whether some memo snapshot recorded a fire at the given nominal
// time (second granularity).
func (o *Observations) HasActionAt(nominal time.Time) bool {
	_, ok := o.RecentActions[nominal.UTC().Truncate(time.Second)]
	return ok
}

// MaxActionNominal returns the latest recorded nominal fire time, zero if none.
func (o *Observations) MaxActionNominal() time.Time {
	var latest time.Time
	for nominal := range o.RecentActions {
		if nominal.After(latest) {
			latest = nominal
		}
	}
	return latest
}

// AllowAllTriggers counts ALLOW_ALL trigger-immediately patches in the recorded history.
func (o *Observations) AllowAllTriggers() int {
	n := 0
	for _, p := range o.Patches {
		if t := p.Patch.GetTriggerImmediately(); t != nil &&
			t.GetOverlapPolicy() == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
			n++
		}
	}
	return n
}

// PointBackfill returns the instant of a single-point backfill (StartTime == EndTime),
// truncated to seconds, if the history recorded one.
func (o *Observations) PointBackfill() (point time.Time, ok bool) {
	for _, p := range o.Patches {
		for _, b := range p.Patch.GetBackfillRequest() {
			start, end := b.GetStartTime().AsTime(), b.GetEndTime().AsTime()
			if start.Equal(end) {
				return start.Truncate(time.Second), true
			}
		}
	}
	return time.Time{}, false
}

// SuppliedTriggerTime returns the explicit ScheduledTime on a trigger-immediately patch,
// truncated to seconds, if the history recorded one.
func (o *Observations) SuppliedTriggerTime() (t time.Time, ok bool) {
	for _, p := range o.Patches {
		if tr := p.Patch.GetTriggerImmediately(); tr != nil && tr.GetScheduledTime() != nil {
			return tr.GetScheduledTime().AsTime().Truncate(time.Second), true
		}
	}
	return time.Time{}, false
}

// SpecEntries returns the largest per-field spec entry count in the continue-as-new input's
// schedule spec (the untrimmed schedule, as opposed to the memo's possibly-trimmed copy).
func (o *Observations) SpecEntries() int {
	spec := o.CANInput.GetSchedule().GetSpec()
	return max(len(spec.GetInterval()), len(spec.GetStructuredCalendar()), len(spec.GetExcludeStructuredCalendar()))
}
