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
)

// TraceEvent is one normalized observation with stable semantic identity and causal references.
type TraceEvent struct {
	Key    string            `json:"key"`
	Kind   TraceKind         `json:"kind"`
	Name   string            `json:"name"`
	Causes []string          `json:"causes,omitempty"`
	Fields map[string]string `json:"fields,omitempty"`
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
