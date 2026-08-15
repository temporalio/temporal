package umpire

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// TraceKind identifies one normalized semantic event category.
type TraceKind string

const (
	TraceFact       TraceKind = "fact"
	TraceTransition TraceKind = "transition"
	TraceRelation   TraceKind = "relation"
	TraceAction     TraceKind = "action"
	TraceVerdict    TraceKind = "verdict"
	TraceRedacted             = "[redacted]"
)

var (
	ErrTraceEvent = errors.New("invalid trace event")
	ErrTraceLimit = errors.New("trace limit exceeded")
	ErrTraceOrder = errors.New("trace order is incomparable")
)

// TraceEvent is one normalized observation with stable semantic identity and causal references.
type TraceEvent struct {
	Key            string            `json:"key"`
	Kind           TraceKind         `json:"kind"`
	Name           string            `json:"name"`
	Source         EvidenceSource    `json:"source,omitempty"`
	ClockDomain    string            `json:"clockDomain,omitempty"`
	SourceSequence uint64            `json:"sourceSequence,omitempty"`
	Causes         []string          `json:"causes,omitempty"`
	Fields         map[string]string `json:"fields,omitempty"`
}

// Trace is a bounded normalized execution artifact.
type Trace struct {
	Events   []TraceEvent `json:"events"`
	Complete bool         `json:"complete"`
}

// TraceOptions bounds retained observations. Zero limits are unlimited.
type TraceOptions struct {
	MaxEvents int
	MaxBytes  int
}

// TraceRecorder retains normalized trace events in observation order.
type TraceRecorder struct {
	mu      sync.RWMutex
	options TraceOptions
	events  []TraceEvent
	keys    map[string]struct{}
	bytes   int
}

// NewTraceRecorder creates an empty opt-in trace recorder.
func NewTraceRecorder(options TraceOptions) *TraceRecorder {
	return &TraceRecorder{options: options, keys: map[string]struct{}{}}
}

// Record normalizes and appends one event without partially changing state on error.
func (r *TraceRecorder) Record(event TraceEvent) error {
	normalized, encodedSize, err := normalizeTraceEvent(event)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.keys[normalized.Key]; exists {
		return fmt.Errorf("%w: duplicate key %q", ErrTraceEvent, normalized.Key)
	}
	if r.options.MaxEvents > 0 && len(r.events) >= r.options.MaxEvents {
		return fmt.Errorf("%w: event count exceeds %d", ErrTraceLimit, r.options.MaxEvents)
	}
	if r.options.MaxBytes > 0 && r.bytes+encodedSize > r.options.MaxBytes {
		return fmt.Errorf("%w: encoded bytes exceed %d", ErrTraceLimit, r.options.MaxBytes)
	}
	r.keys[normalized.Key] = struct{}{}
	r.events = append(r.events, normalized)
	r.bytes += encodedSize
	return nil
}

func normalizeTraceEvent(event TraceEvent) (TraceEvent, int, error) {
	if event.Key == "" || event.Name == "" || !validTraceKind(event.Kind) {
		return TraceEvent{}, 0, fmt.Errorf("%w: key, kind, and name are required", ErrTraceEvent)
	}
	event.Causes = append([]string(nil), event.Causes...)
	slices.Sort(event.Causes)
	event.Causes = compactStrings(event.Causes)
	if event.Fields != nil {
		fields := make(map[string]string, len(event.Fields))
		for key, value := range event.Fields {
			if sensitiveTraceField(key) {
				value = TraceRedacted
			}
			fields[key] = value
		}
		event.Fields = fields
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return TraceEvent{}, 0, fmt.Errorf("%w: encode: %v", ErrTraceEvent, err)
	}
	return event, len(payload), nil
}

func validTraceKind(kind TraceKind) bool {
	switch kind {
	case TraceFact, TraceTransition, TraceRelation, TraceAction, TraceVerdict:
		return true
	default:
		return false
	}
}

func compactStrings(values []string) []string {
	result := values[:0]
	for _, value := range values {
		if value == "" || len(result) > 0 && result[len(result)-1] == value {
			continue
		}
		result = append(result, value)
	}
	return result
}

func sensitiveTraceField(key string) bool {
	lower := strings.ToLower(key)
	for _, fragment := range []string{"payload", "authorization", "credential", "token"} {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

// Snapshot returns an immutable copy of the retained trace.
func (r *TraceRecorder) Snapshot() Trace {
	if r == nil {
		return Trace{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := Trace{Events: make([]TraceEvent, len(r.events))}
	for index, event := range r.events {
		result.Events[index] = cloneTraceEvent(event)
	}
	return result
}

func cloneTraceEvent(event TraceEvent) TraceEvent {
	event.Causes = append([]string(nil), event.Causes...)
	if event.Fields != nil {
		fields := make(map[string]string, len(event.Fields))
		for key, value := range event.Fields {
			fields[key] = value
		}
		event.Fields = fields
	}
	return event
}

// TracePattern selects events by semantic kind and name.
type TracePattern struct {
	Kind TraceKind
	Name string
}

// TraceRefinement declares required ordered observations and forbidden observations.
type TraceRefinement struct {
	Required    []TracePattern
	Forbidden   []TracePattern
	AllowExtras bool
}

// CausalFootprint declares the normalized observations required inside one action window.
type CausalFootprint struct {
	Action     string
	Refinement TraceRefinement
	Causal     []TracePattern
}

// TraceMismatch describes the first refinement or causality mismatch.
type TraceMismatch struct {
	Index   int
	Reason  string
	Pattern TracePattern
	Event   TraceEvent
}

func (e *TraceMismatch) Error() string {
	return fmt.Sprintf("trace mismatch at %d: %s", e.Index, e.Reason)
}

// CompareTraceRefinement checks forbidden events, causality, and required semantic ordering.
func CompareTraceRefinement(spec TraceRefinement, actual Trace) error {
	for index, event := range actual.Events {
		for _, forbidden := range spec.Forbidden {
			if traceMatches(forbidden, event) {
				return &TraceMismatch{Index: index, Reason: "forbidden observation", Pattern: forbidden, Event: event}
			}
		}
	}
	seen := map[string]struct{}{}
	for index, event := range actual.Events {
		if event.Key == "" {
			return &TraceMismatch{Index: index, Reason: "event key is empty", Event: event}
		}
		if _, duplicate := seen[event.Key]; duplicate {
			return &TraceMismatch{Index: index, Reason: "duplicate event key", Event: event}
		}
		for _, cause := range event.Causes {
			if _, exists := seen[cause]; !exists {
				return &TraceMismatch{Index: index, Reason: fmt.Sprintf("cause %q is missing or occurs later", cause), Event: event}
			}
		}
		seen[event.Key] = struct{}{}
	}
	position := 0
	for _, required := range spec.Required {
		found := -1
		for index := position; index < len(actual.Events); index++ {
			if traceMatches(required, actual.Events[index]) {
				found = index
				break
			}
		}
		if found < 0 {
			return &TraceMismatch{Index: position, Reason: "required observation is missing or misordered", Pattern: required}
		}
		position = found + 1
	}
	if !spec.AllowExtras && len(actual.Events) != len(spec.Required) {
		return &TraceMismatch{Index: position, Reason: "unexpected extra observation"}
	}
	return nil
}

// CompareTraceRefinementWithEvidence requires complete profile-valid evidence and establishes
// ordering only through causality or a source sequence. Observation order and wall-clock fields
// alone never establish causality.
func CompareTraceRefinementWithEvidence(spec TraceRefinement, actual Trace, profile EnvironmentProfile) error {
	if err := ValidateTraceEvidence(actual, profile); err != nil {
		return err
	}
	if err := CompareTraceRefinement(TraceRefinement{Forbidden: spec.Forbidden, AllowExtras: true}, actual); err != nil {
		return err
	}
	positions := make([]int, 0, len(spec.Required))
	used := map[int]struct{}{}
	for _, required := range spec.Required {
		found := -1
		for index, event := range actual.Events {
			if _, exists := used[index]; !exists && traceMatches(required, event) {
				found = index
				break
			}
		}
		if found < 0 {
			return &TraceMismatch{Reason: "required observation is missing", Pattern: required}
		}
		used[found] = struct{}{}
		positions = append(positions, found)
	}
	for index := 1; index < len(positions); index++ {
		before := actual.Events[positions[index-1]]
		after := actual.Events[positions[index]]
		ordered, err := traceOrderedBefore(actual, before.Key, after.Key)
		if err != nil {
			return err
		}
		if !ordered {
			return &TraceMismatch{Index: positions[index], Reason: fmt.Sprintf("%v: %q and %q", ErrTraceOrder, before.Key, after.Key), Pattern: spec.Required[index], Event: after}
		}
	}
	if !spec.AllowExtras && len(actual.Events) != len(spec.Required) {
		return &TraceMismatch{Index: len(positions), Reason: "unexpected extra observation"}
	}
	return nil
}

func validateTraceEvidenceProfile(trace Trace, profile EnvironmentProfile) error {
	if err := ValidateEnvironmentProfile(profile); err != nil {
		return err
	}
	available := makeSet(profile.ObservationSources)
	domains := make(map[EvidenceSource]map[string]struct{})
	for _, domain := range profile.ClockDomains {
		for _, source := range domain.Sources {
			if domains[source] == nil {
				domains[source] = map[string]struct{}{}
			}
			domains[source][domain.Name] = struct{}{}
		}
	}
	for index, event := range trace.Events {
		if event.Name == "" || !validTraceKind(event.Kind) {
			return &TraceMismatch{Index: index, Reason: "event kind and name are required", Event: event}
		}
		if _, exists := available[event.Source]; !exists {
			return &TraceMismatch{Index: index, Reason: fmt.Sprintf("source %q is unavailable in profile %q", event.Source, profile.Name), Event: event}
		}
		if event.ClockDomain != "" {
			if _, exists := domains[event.Source][event.ClockDomain]; !exists {
				return &TraceMismatch{Index: index, Reason: fmt.Sprintf("clock domain %q is not declared for source %q", event.ClockDomain, event.Source), Event: event}
			}
		}
		if event.SourceSequence > 0 && (!slices.Contains(profile.OrderingGuarantees, SourceSequenceOrdering) || event.ClockDomain == "") {
			return &TraceMismatch{Index: index, Reason: "source sequence lacks a declared ordering guarantee and clock domain", Event: event}
		}
		if len(event.Causes) > 0 && !slices.Contains(profile.OrderingGuarantees, CausalOrdering) {
			return &TraceMismatch{Index: index, Reason: "causal reference lacks a declared ordering guarantee", Event: event}
		}
	}
	return nil
}

// ValidateTraceEvidence checks that retained evidence is complete and valid for its profile.
func ValidateTraceEvidence(trace Trace, profile EnvironmentProfile) error {
	if !trace.Complete {
		return fmt.Errorf("%w: retained trace is incomplete", ErrTraceOrder)
	}
	if err := validateTraceEvidenceProfile(trace, profile); err != nil {
		return err
	}
	return CompareTraceRefinement(TraceRefinement{AllowExtras: true}, trace)
}

// TraceOrderedBefore reports whether complete profile-valid evidence establishes that before
// causally precedes after.
func TraceOrderedBefore(trace Trace, profile EnvironmentProfile, before, after string) (bool, error) {
	if err := ValidateTraceEvidence(trace, profile); err != nil {
		return false, err
	}
	return traceOrderedBefore(trace, before, after)
}

func traceOrderedBefore(trace Trace, before, after string) (bool, error) {
	if before == "" || after == "" || before == after {
		return false, fmt.Errorf("%w: distinct event keys are required", ErrTraceOrder)
	}
	events := make(map[string]TraceEvent, len(trace.Events))
	for _, event := range trace.Events {
		if _, exists := events[event.Key]; exists {
			return false, fmt.Errorf("%w: duplicate event key %q", ErrTraceEvent, event.Key)
		}
		events[event.Key] = event
	}
	left, leftExists := events[before]
	right, rightExists := events[after]
	if !leftExists || !rightExists {
		return false, fmt.Errorf("%w: event key is missing", ErrTraceOrder)
	}
	causal := traceCausallyDepends(events, after, before)
	reverseCausal := traceCausallyDepends(events, before, after)
	if causal && reverseCausal {
		return false, fmt.Errorf("%w: causal reference cycle between %q and %q", ErrTraceOrder, before, after)
	}
	if left.ClockDomain != "" && left.ClockDomain == right.ClockDomain && left.SourceSequence > 0 && right.SourceSequence > 0 {
		sequence := left.SourceSequence < right.SourceSequence
		if causal && !sequence || reverseCausal && sequence {
			return false, fmt.Errorf("%w: causal references conflict with source sequence", ErrTraceOrder)
		}
		return sequence, nil
	}
	return causal, nil
}

func traceCausallyDepends(events map[string]TraceEvent, descendant, ancestor string) bool {
	visited := map[string]struct{}{}
	var visit func(string) bool
	visit = func(key string) bool {
		if key == ancestor {
			return true
		}
		if _, seen := visited[key]; seen {
			return false
		}
		visited[key] = struct{}{}
		for _, cause := range events[key].Causes {
			if visit(cause) {
				return true
			}
		}
		return false
	}
	return visit(descendant)
}

// CompareCausalFootprint checks one action window's refinement and required action-start causes.
func CompareCausalFootprint(spec CausalFootprint, actual Trace) error {
	if spec.Action == "" {
		return &TraceMismatch{Reason: "action name is empty"}
	}
	if err := CompareTraceRefinement(TraceRefinement{AllowExtras: true}, actual); err != nil {
		return err
	}
	start := -1
	finish := -1
	var pending []int
	for index, event := range actual.Events {
		if event.Kind != TraceAction || event.Name != spec.Action {
			continue
		}
		if event.Fields["outcome"] == ExecutionOutcomeStarted {
			pending = append(pending, index)
			continue
		}
		if len(pending) != 0 {
			start = pending[0]
			pending = pending[1:]
			finish = index
		}
	}
	if finish < 0 && len(pending) != 0 {
		start = pending[0]
	}
	if start < 0 {
		return &TraceMismatch{Reason: "action window start is missing", Pattern: TracePattern{Kind: TraceAction, Name: spec.Action}}
	}
	if finish < 0 {
		return &TraceMismatch{Index: start + 1, Reason: "action window finish is missing", Pattern: TracePattern{Kind: TraceAction, Name: spec.Action}}
	}
	startKey := actual.Events[start].Key
	for _, required := range spec.Causal {
		matched := false
		for index := start + 1; index < finish; index++ {
			event := actual.Events[index]
			if !traceMatches(required, event) {
				continue
			}
			matched = true
			if !slices.Contains(event.Causes, startKey) {
				return &TraceMismatch{Index: index, Reason: "observation is causally disconnected from action start", Pattern: required, Event: event}
			}
		}
		if !matched {
			return &TraceMismatch{Index: start + 1, Reason: "causal observation is missing", Pattern: required}
		}
	}
	seenSemantic := map[TracePattern]struct{}{}
	for index := start + 1; index < finish; index++ {
		event := actual.Events[index]
		pattern := TracePattern{Kind: event.Kind, Name: event.Name}
		if !slices.Contains(spec.Causal, pattern) {
			continue
		}
		if _, duplicate := seenSemantic[pattern]; duplicate {
			return &TraceMismatch{Index: index, Reason: "duplicate semantic observation", Pattern: pattern, Event: event}
		}
		seenSemantic[pattern] = struct{}{}
	}
	window := Trace{Events: make([]TraceEvent, finish-start+1)}
	for index, event := range actual.Events[start : finish+1] {
		window.Events[index] = cloneTraceEvent(event)
		window.Events[index].Causes = nil
	}
	refinement := spec.Refinement
	refinement.Required = append([]TracePattern{{Kind: TraceAction, Name: spec.Action}}, refinement.Required...)
	refinement.Required = append(refinement.Required, TracePattern{Kind: TraceAction, Name: spec.Action})
	return CompareTraceRefinement(refinement, window)
}

func traceMatches(pattern TracePattern, event TraceEvent) bool {
	return pattern.Kind == event.Kind && pattern.Name == event.Name
}

// WriteTraceFile atomically replaces a normalized JSON trace artifact.
func WriteTraceFile(path string, trace Trace) (resultErr error) {
	if path == "" {
		return errors.New("trace path is empty")
	}
	directory := filepath.Dir(filepath.Clean(path))
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create trace directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".umpire-trace-*.json")
	if err != nil {
		return fmt.Errorf("create temporary trace: %w", err)
	}
	temporaryPath := temporary.Name()
	temporaryOpen := true
	removeTemporary := true
	defer func() {
		if temporaryOpen {
			resultErr = errors.Join(resultErr, temporary.Close())
		}
		if removeTemporary {
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}()
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(trace); err != nil {
		return fmt.Errorf("encode trace: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("flush trace: %w", err)
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return fmt.Errorf("close trace: %w", err)
	}
	temporaryOpen = false
	if err := os.Rename(temporaryPath, filepath.Clean(path)); err != nil {
		return fmt.Errorf("replace trace: %w", err)
	}
	removeTemporary = false
	return nil
}
