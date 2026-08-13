package verify

import (
	"errors"
	"fmt"
)

type Status string

const (
	Generated               Status = "generated"
	BoundedNoCounterexample Status = "bounded-no-counterexample"
	FiniteExhaustive        Status = "finite-exhaustive"
	InvariantProved         Status = "invariant-proved"
	Counterexample          Status = "counterexample"
	UnsupportedStatus       Status = "unsupported"
	Inconclusive            Status = "inconclusive"
)

type TerminationReason string

const (
	Completed    TerminationReason = "completed"
	Timeout      TerminationReason = "timeout"
	DepthLimit   TerminationReason = "depth-limit"
	StateLimit   TerminationReason = "state-limit"
	StepLimit    TerminationReason = "step-limit"
	MemoryLimit  TerminationReason = "memory-limit"
	ToolError    TerminationReason = "tool-error"
	Interrupted  TerminationReason = "interrupted"
	ParseFailure TerminationReason = "parse-failure"
)

type Bounds struct {
	Identities map[string]int `json:"identities,omitempty"`
	MaxDepth   uint64         `json:"maxDepth,omitempty"`
	Schedules  uint64         `json:"schedules,omitempty"`
	MemoryGB   float64        `json:"memoryGB,omitempty"`
}

type StateDelta struct {
	Entity    string `json:"entity,omitempty"`
	ID        string `json:"id,omitempty"`
	FromState string `json:"fromState,omitempty"`
	ToState   string `json:"toState,omitempty"`
	Relation  string `json:"relation,omitempty"`
	Source    string `json:"source,omitempty"`
	Target    string `json:"target,omitempty"`
	Added     bool   `json:"added,omitempty"`
}

type TraceStep struct {
	Action   string       `json:"action"`
	Bindings Bindings     `json:"bindings,omitempty"`
	Deltas   []StateDelta `json:"deltas,omitempty"`
}

type Result struct {
	Backend         string            `json:"backend"`
	Target          string            `json:"target,omitempty"`
	Profile         string            `json:"profile,omitempty"`
	ToolVersion     string            `json:"toolVersion,omitempty"`
	Status          Status            `json:"status"`
	Termination     TerminationReason `json:"termination"`
	FailedProperty  string            `json:"failedProperty,omitempty"`
	Bounds          Bounds            `json:"bounds,omitempty"`
	GeneratedStates uint64            `json:"generatedStates,omitempty"`
	DistinctStates  uint64            `json:"distinctStates,omitempty"`
	Fairness        []string          `json:"fairness,omitempty"`
	Abstractions    []Abstraction     `json:"abstractions,omitempty"`
	Unsupported     []Unsupported     `json:"unsupported,omitempty"`
	Trace           []TraceStep       `json:"trace,omitempty"`
	Artifacts       []string          `json:"artifacts,omitempty"`
	ReplayCommand   []string          `json:"replayCommand,omitempty"`
	ReplayCommands  [][]string        `json:"replayCommands,omitempty"`
	StandardOutput  string            `json:"standardOutput,omitempty"`
	StandardError   string            `json:"standardError,omitempty"`
	Diagnostic      string            `json:"diagnostic,omitempty"`
}

func ValidateResult(result Result) error {
	if result.Status == "" {
		return errors.New("verification result status is empty")
	}
	if result.Status == Generated || result.Status == BoundedNoCounterexample || result.Status == FiniteExhaustive || result.Status == InvariantProved {
		switch result.Termination {
		case Timeout, DepthLimit, StateLimit, StepLimit, MemoryLimit, ToolError, Interrupted, ParseFailure:
			return fmt.Errorf("status %q cannot claim success after %q", result.Status, result.Termination)
		default:
		}
	}
	if result.Status == Counterexample && result.FailedProperty == "" {
		return errors.New("counterexample has no failed property")
	}
	return nil
}
