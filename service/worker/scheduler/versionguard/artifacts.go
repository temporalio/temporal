// Package versionguard freezes the V1 scheduler's per-version decision procedure.
//
// Each SchedulerWorkflowVersion gates behavior that must never change once shipped (the
// frozen-gate invariant the worker.schedulerVersionCeiling dynamic config relies on). This
// package defines, for every version, a token: history-observable proof that the version's
// gate executed on a particular side. Golden history snapshots, one per
// (version, scenario), are generated against a real server with the ceiling clamped to
// that version and committed under service/worker/scheduler/testdata. Two guards then hold
// forever:
//
//   - Replay: TestReplays replays every snapshot under the current binary, so an ungated
//     behavior change breaks the frozen versions it reaches.
//   - Tokens: Evaluate asserts every snapshot exhibits each version token on the correct
//     side (On at or below the clamp, Off above it) and that no token is unexercised, so a
//     snapshot can never silently stop covering the gate it exists to freeze.
//
// Adding version N+1: add a Token entry here, extend (or add) a scenario in the generator
// so the token is exercised, and generate the new snapshots. The completeness test fails
// with instructions until the snapshots exist; existing snapshots must never be
// regenerated (deleting one is a deliberate, review-visible act).
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

// Artifacts is the single-pass projection of a scheduler workflow history into the facts
// the version tokens are defined over. Detectors read this instead of raw events so each
// stays a few lines and new tokens compose existing facts.
type Artifacts struct {
	// TweakableVersions holds the Version recorded by each "tweakables" MutableSideEffect
	// marker, in history order. TweakableEncodings holds the matching payload encodings.
	TweakableVersions  []int64
	TweakableEncodings []string

	// Next-time cache SideEffect markers by wire format, plus the pre-cache V1 batched
	// time-query markers.
	ProtoCacheMarkers  int
	JSONCacheMarkers   int
	V1BatchTimeQueries int

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

	// Patches and Updates are the schedule signals received, with event timestamps.
	Patches []TimedPatch
	Updates []TimedUpdate

	// CANInput is the StartScheduleArgs carried by the run's continue-as-new event, nil if
	// the run did not continue-as-new.
	CANInput *schedulespb.StartScheduleArgs
}

type TimedPatch struct {
	Time  time.Time
	Patch *schedulepb.SchedulePatch
}

type TimedUpdate struct {
	Time   time.Time
	Update *schedulespb.FullUpdateRequest
}

const (
	markerSideEffect        = "SideEffect"
	markerMutableSideEffect = "MutableSideEffect"
	memoFieldScheduleInfo   = "ScheduleInfo"
	protoCacheMessageType   = "temporal.server.api.schedule.v1.NextTimeCache"
	encodingJSON            = "json/plain"
)

// Parse projects a scheduler workflow history into Artifacts.
func Parse(events []*historypb.HistoryEvent) (*Artifacts, error) {
	a := &Artifacts{
		RecentActions: map[time.Time]enumspb.WorkflowExecutionStatus{},
		SAUpserts:     map[string]int{},
	}
	dc := converter.GetDefaultDataConverter()
	for _, event := range events {
		switch {
		case event.GetMarkerRecordedEventAttributes() != nil:
			if err := a.parseMarker(dc, event.GetMarkerRecordedEventAttributes()); err != nil {
				return nil, fmt.Errorf("event %d: %w", event.GetEventId(), err)
			}
		case event.GetWorkflowExecutionStartedEventAttributes() != nil:
			a.parseMemo(event.GetWorkflowExecutionStartedEventAttributes().GetMemo())
		case event.GetWorkflowPropertiesModifiedEventAttributes() != nil:
			a.parseMemo(event.GetWorkflowPropertiesModifiedEventAttributes().GetUpsertedMemo())
		case event.GetUpsertWorkflowSearchAttributesEventAttributes() != nil:
			for key := range event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields() {
				a.SAUpserts[key]++
			}
		case event.GetWorkflowExecutionSignaledEventAttributes() != nil:
			if err := a.parseSignal(event); err != nil {
				return nil, fmt.Errorf("event %d: %w", event.GetEventId(), err)
			}
		case event.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
			var args schedulespb.StartScheduleArgs
			if err := payloads.Decode(event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetInput(), &args); err != nil {
				return nil, fmt.Errorf("event %d: decoding continue-as-new input: %w", event.GetEventId(), err)
			}
			a.CANInput = &args
		default:
			// Other event types carry no version artifacts.
		}
	}
	return a, nil
}

func (a *Artifacts) parseMarker(dc converter.DataConverter, marker *historypb.MarkerRecordedEventAttributes) error {
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
		a.TweakableVersions = append(a.TweakableVersions, policies.Version)
		a.TweakableEncodings = append(a.TweakableEncodings, string(value.Metadata["encoding"]))
	case markerSideEffect:
		data := marker.GetDetails()["data"]
		if data == nil || len(data.Payloads) == 0 {
			return nil
		}
		a.classifySideEffect(data.Payloads[0])
	default:
		// Other markers (local activities, versions) carry no version artifacts.
	}
	return nil
}

// classifySideEffect buckets a plain SideEffect payload into the three time-query shapes
// the scheduler has used across versions. Payloads that match none (e.g. the UUID batch)
// are ignored.
func (a *Artifacts) classifySideEffect(p *commonpb.Payload) {
	if string(p.Metadata["messageType"]) == protoCacheMessageType {
		a.ProtoCacheMarkers++
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
			a.JSONCacheMarkers++
			return
		}
	}
	// The pre-cache V1 batch is a map keyed by RFC3339 query times.
	for key := range obj {
		if _, err := time.Parse(time.RFC3339, key); err == nil {
			a.V1BatchTimeQueries++
			return
		}
	}
}

func (a *Artifacts) parseMemo(memo *commonpb.Memo) {
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
			a.RecentActions[nominal] == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			a.RecentActions[nominal] = action.GetStartWorkflowStatus()
		}
	}
	a.MaxFutureActionTimes = max(a.MaxFutureActionTimes, len(info.FutureActionTimes))
	spec := info.GetSpec()
	a.MaxMemoSpecEntries = max(a.MaxMemoSpecEntries,
		len(spec.GetInterval()), len(spec.GetStructuredCalendar()), len(spec.GetExcludeStructuredCalendar()))
}

func (a *Artifacts) parseSignal(event *historypb.HistoryEvent) error {
	attrs := event.GetWorkflowExecutionSignaledEventAttributes()
	at := event.GetEventTime().AsTime()
	switch attrs.GetSignalName() {
	case scheduler.SignalNamePatch:
		var patch schedulepb.SchedulePatch
		if err := payloads.Decode(attrs.GetInput(), &patch); err != nil {
			return fmt.Errorf("decoding patch signal: %w", err)
		}
		a.Patches = append(a.Patches, TimedPatch{Time: at, Patch: &patch})
	case scheduler.SignalNameUpdate:
		var update schedulespb.FullUpdateRequest
		if err := payloads.Decode(attrs.GetInput(), &update); err != nil {
			return fmt.Errorf("decoding update signal: %w", err)
		}
		a.Updates = append(a.Updates, TimedUpdate{Time: at, Update: &update})
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
func (a *Artifacts) HasActionAt(nominal time.Time) bool {
	_, ok := a.RecentActions[nominal.UTC().Truncate(time.Second)]
	return ok
}

// HasActionIn reports whether some fire's nominal time falls inside [from, to).
func (a *Artifacts) HasActionIn(from, to time.Time) bool {
	for nominal := range a.RecentActions {
		if !nominal.Before(from) && nominal.Before(to) {
			return true
		}
	}
	return false
}

// MaxActionNominal returns the latest recorded nominal fire time, zero if none.
func (a *Artifacts) MaxActionNominal() time.Time {
	var latest time.Time
	for nominal := range a.RecentActions {
		if nominal.After(latest) {
			latest = nominal
		}
	}
	return latest
}
