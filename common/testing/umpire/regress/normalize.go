package regress

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// ErrorCategory is a stable classification for sparse-plan compilation failures.
type ErrorCategory string

const (
	ErrorSymbolTypeConflict               ErrorCategory = "symbol type conflict"
	ErrorInvalidInstruction               ErrorCategory = "invalid instruction"
	ErrorContradictoryOrdering            ErrorCategory = "contradictory ordering"
	ErrorInvalidPolicyLifetime            ErrorCategory = "invalid policy lifetime"
	ErrorUnreachableOutcome               ErrorCategory = "unreachable outcome"
	ErrorUnavailableEnvironmentCapability ErrorCategory = "unavailable environment capability"
	ErrorMissingModelCapability           ErrorCategory = "missing model capability"
	ErrorAmbiguousGrounding               ErrorCategory = "ambiguous grounding"
	ErrorUnboundedCycle                   ErrorCategory = "unbounded cycle"
	ErrorIncompleteAllPaths               ErrorCategory = "incomplete AllPaths enumeration"
	ErrorInvalidCompletedSuite            ErrorCategory = "invalid completed suite"
	ErrorMissingRealization               ErrorCategory = "missing realization"
	ErrorRealizationModeMismatch          ErrorCategory = "realization mode mismatch"
	ErrorMissingResource                  ErrorCategory = "missing resource"
	ErrorResourceDependencyCycle          ErrorCategory = "resource dependency cycle"
	ErrorInvalidRealizationCatalog        ErrorCategory = "invalid realization catalog"
)

var (
	ErrSymbolTypeConflict               = errors.New(string(ErrorSymbolTypeConflict))
	ErrInvalidInstruction               = errors.New(string(ErrorInvalidInstruction))
	ErrContradictoryOrdering            = errors.New(string(ErrorContradictoryOrdering))
	ErrInvalidPolicyLifetime            = errors.New(string(ErrorInvalidPolicyLifetime))
	ErrUnreachableOutcome               = errors.New(string(ErrorUnreachableOutcome))
	ErrUnavailableEnvironmentCapability = errors.New(string(ErrorUnavailableEnvironmentCapability))
	ErrMissingModelCapability           = errors.New(string(ErrorMissingModelCapability))
	ErrAmbiguousGrounding               = errors.New(string(ErrorAmbiguousGrounding))
	ErrUnboundedCycle                   = errors.New(string(ErrorUnboundedCycle))
	ErrIncompleteAllPaths               = errors.New(string(ErrorIncompleteAllPaths))
	ErrInvalidCompletedSuite            = errors.New(string(ErrorInvalidCompletedSuite))
	ErrMissingRealization               = errors.New(string(ErrorMissingRealization))
	ErrRealizationModeMismatch          = errors.New(string(ErrorRealizationModeMismatch))
	ErrMissingResource                  = errors.New(string(ErrorMissingResource))
	ErrResourceDependencyCycle          = errors.New(string(ErrorResourceDependencyCycle))
	ErrInvalidRealizationCatalog        = errors.New(string(ErrorInvalidRealizationCatalog))
)

// CompileError retains source and causal information for a planning failure.
type CompileError struct {
	Category     ErrorCategory
	Source       int
	Related      []int
	Symbol       string
	Expected     string
	Actual       string
	Detail       string
	Predicate    string
	Candidates   []string
	MissingChain []string
}

func (e *CompileError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s at instruction %d: %s", e.Category, e.Source, e.Detail)
	}
	return fmt.Sprintf("%s at instruction %d: %s is %s, previously %s", e.Category, e.Source, e.Symbol, e.Expected, e.Actual)
}

func (e *CompileError) Unwrap() error {
	switch e.Category {
	case ErrorSymbolTypeConflict:
		return ErrSymbolTypeConflict
	case ErrorContradictoryOrdering:
		return ErrContradictoryOrdering
	case ErrorInvalidPolicyLifetime:
		return ErrInvalidPolicyLifetime
	case ErrorUnreachableOutcome:
		return ErrUnreachableOutcome
	case ErrorUnavailableEnvironmentCapability:
		return ErrUnavailableEnvironmentCapability
	case ErrorMissingModelCapability:
		return ErrMissingModelCapability
	case ErrorAmbiguousGrounding:
		return ErrAmbiguousGrounding
	case ErrorUnboundedCycle:
		return ErrUnboundedCycle
	case ErrorIncompleteAllPaths:
		return ErrIncompleteAllPaths
	case ErrorInvalidCompletedSuite:
		return ErrInvalidCompletedSuite
	case ErrorMissingRealization:
		return ErrMissingRealization
	case ErrorRealizationModeMismatch:
		return ErrRealizationModeMismatch
	case ErrorMissingResource:
		return ErrMissingResource
	case ErrorResourceDependencyCycle:
		return ErrResourceDependencyCycle
	case ErrorInvalidRealizationCatalog:
		return ErrInvalidRealizationCatalog
	default:
		return ErrInvalidInstruction
	}
}

// SymbolInfo records one consistently typed symbolic name and every source that uses it.
type SymbolInfo struct {
	Name        string `json:"name"`
	Type        Type   `json:"type"`
	FirstSource int    `json:"firstSource"`
	Uses        []int  `json:"uses"`
}

type Symbols map[string]SymbolInfo

// Node is one normalized semantic key frame.
type Node struct {
	ID        int             `json:"id"`
	Source    int             `json:"source"`
	Kind      InstructionKind `json:"kind"`
	Name      string          `json:"name"`
	Arguments []Argument      `json:"arguments,omitempty"`
	Binding   string          `json:"binding,omitempty"`
}

// Edge orders one normalized key frame before another.
type Edge struct {
	From int `json:"from"`
	To   int `json:"to"`
}

// PolicyIR is a normalized policy installed around one scope body.
type PolicyIR struct {
	Source    int        `json:"source"`
	Name      string     `json:"name"`
	Arguments []Argument `json:"arguments,omitempty"`
}

// Scope gives a policy the exact normalized nodes whose synthesized behavior it covers.
type Scope struct {
	ID     int      `json:"id"`
	Policy PolicyIR `json:"policy"`
	Body   []int    `json:"body"`
}

// RequirementIR is a profile constraint that does not participate in milestone ordering.
type RequirementIR struct {
	Source    int        `json:"source"`
	Name      string     `json:"name"`
	Arguments []Argument `json:"arguments,omitempty"`
}

// IR is the immutable, domain-independent normalized form of a sparse plan.
type IR struct {
	Mode         PathMode        `json:"mode"`
	Symbols      Symbols         `json:"symbols"`
	Nodes        []Node          `json:"nodes"`
	Edges        []Edge          `json:"edges,omitempty"`
	Scopes       []Scope         `json:"scopes,omitempty"`
	Labels       map[string]int  `json:"labels,omitempty"`
	Requirements []RequirementIR `json:"requirements,omitempty"`
}

func (ir IR) String() string {
	encoded, err := MarshalIR(ir)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// MarshalIR returns the stable JSON artifact representation of normalized sparse intent.
func MarshalIR(ir IR) ([]byte, error) {
	return json.Marshal(ir)
}

// UnmarshalIR restores and validates normalized sparse intent from an artifact.
func UnmarshalIR(encoded []byte) (IR, error) {
	var ir IR
	if err := json.Unmarshal(encoded, &ir); err != nil {
		return IR{}, fmt.Errorf("decode sparse regression IR: %w", err)
	}
	if ir.Symbols == nil {
		ir.Symbols = Symbols{}
	}
	if ir.Labels == nil {
		ir.Labels = map[string]int{}
	}
	for index, node := range ir.Nodes {
		if node.ID != index {
			return IR{}, fmt.Errorf("decode sparse regression IR: node %d has id %d", index, node.ID)
		}
	}
	if hasCycle(len(ir.Nodes), ir.Edges) {
		return IR{}, &CompileError{Category: ErrorContradictoryOrdering, Detail: "ordering constraints contain a cycle"}
	}
	return ir, nil
}

// Normalize type-checks and lowers the sparse instruction list to a constraint DAG.
func Normalize(plan Plan) (IR, error) {
	n := normalizer{
		ir:    IR{Mode: plan.Mode, Symbols: Symbols{}, Labels: map[string]int{}},
		edges: map[Edge]struct{}{},
	}
	if _, err := n.sequence(plan.instructions); err != nil {
		return IR{}, err
	}
	if err := n.applyBindingEdges(); err != nil {
		return IR{}, err
	}
	if err := n.applyBefore(); err != nil {
		return IR{}, err
	}
	for edge := range n.edges {
		n.ir.Edges = append(n.ir.Edges, edge)
	}
	slices.SortFunc(n.ir.Edges, func(left, right Edge) int {
		if left.From != right.From {
			return cmp.Compare(left.From, right.From)
		}
		return cmp.Compare(left.To, right.To)
	})
	if hasCycle(len(n.ir.Nodes), n.ir.Edges) {
		return IR{}, &CompileError{Category: ErrorContradictoryOrdering, Detail: "ordering constraints contain a cycle"}
	}
	return n.ir, nil
}

type fragment struct {
	entries []int
	exits   []int
}

type beforeConstraint struct {
	source int
	from   string
	to     string
}

type normalizer struct {
	ir     IR
	source int
	edges  map[Edge]struct{}
	before []beforeConstraint
}

func (n *normalizer) sequence(instructions []Instruction) (fragment, error) {
	var result fragment
	for _, raw := range instructions {
		current, err := n.normalize(raw.instruction())
		if err != nil {
			return fragment{}, err
		}
		if len(current.entries) == 0 {
			continue
		}
		if len(result.entries) == 0 {
			result = current
			continue
		}
		n.connect(result.exits, current.entries)
		result.exits = current.exits
	}
	return result, nil
}

func (n *normalizer) normalize(value instructionValue) (fragment, error) {
	switch value.class {
	case atomicInstruction, bindInstruction:
		n.source++
		if value.class == atomicInstruction && value.schema.Kind == RequirementKind {
			if err := collectArguments(n.ir.Symbols, value.schema, value.arguments, n.source); err != nil {
				return fragment{}, err
			}
			n.ir.Requirements = append(n.ir.Requirements, RequirementIR{
				Source:    n.source,
				Name:      value.schema.Name,
				Arguments: append([]Argument(nil), value.arguments...),
			})
			return fragment{}, nil
		}
		if value.class == atomicInstruction && value.schema.Kind == PolicyKind {
			return fragment{}, &CompileError{Category: ErrorInvalidPolicyLifetime, Source: n.source, Detail: "policy must be scoped by During"}
		}
		node, err := normalizeAtomic(&n.ir, value, n.source)
		if err != nil {
			return fragment{}, err
		}
		node.ID = len(n.ir.Nodes)
		n.ir.Nodes = append(n.ir.Nodes, node)
		return fragment{entries: []int{node.ID}, exits: []int{node.ID}}, nil
	case anyOrderInstruction:
		var result fragment
		for _, child := range value.children {
			current, err := n.normalize(child.instruction())
			if err != nil {
				return fragment{}, err
			}
			result.entries = append(result.entries, current.entries...)
			result.exits = append(result.exits, current.exits...)
		}
		return result, nil
	case duringInstruction:
		return n.normalizeDuring(value)
	case stepInstruction:
		if value.label == "" || len(value.children) != 1 {
			return fragment{}, invalidInstruction(n.source+1, "Step requires a label and one instruction")
		}
		current, err := n.normalize(value.children[0].instruction())
		if err != nil {
			return fragment{}, err
		}
		if len(current.entries) != 1 || len(current.exits) != 1 || current.entries[0] != current.exits[0] {
			return fragment{}, invalidInstruction(n.source, "Step must label exactly one instruction occurrence")
		}
		if _, exists := n.ir.Labels[value.label]; exists {
			return fragment{}, invalidInstruction(n.source, fmt.Sprintf("duplicate step label %q", value.label))
		}
		n.ir.Labels[value.label] = current.entries[0]
		return current, nil
	case beforeInstruction:
		n.source++
		n.before = append(n.before, beforeConstraint{source: n.source, from: value.label, to: value.before})
		return fragment{}, nil
	case repeatInstruction:
		if value.repetitions <= 0 {
			return fragment{}, &CompileError{Category: ErrorUnboundedCycle, Source: n.source + 1, Detail: "Repeat requires a positive finite bound"}
		}
		var repeated []Instruction
		for range value.repetitions {
			repeated = append(repeated, value.children...)
		}
		return n.sequence(repeated)
	default:
		return fragment{}, invalidInstruction(n.source+1, "unknown instruction")
	}
}

func (n *normalizer) normalizeDuring(value instructionValue) (fragment, error) {
	if value.policy == nil {
		return fragment{}, &CompileError{Category: ErrorInvalidPolicyLifetime, Source: n.source + 1, Detail: "During requires a policy"}
	}
	policy := value.policy.instruction()
	n.source++
	policySource := n.source
	if policy.class != atomicInstruction || policy.schema.Kind != PolicyKind {
		return fragment{}, &CompileError{Category: ErrorInvalidPolicyLifetime, Source: policySource, Detail: "During requires a policy instruction"}
	}
	if err := collectArguments(n.ir.Symbols, policy.schema, policy.arguments, policySource); err != nil {
		return fragment{}, err
	}
	firstNode := len(n.ir.Nodes)
	body, err := n.sequence(value.children)
	if err != nil {
		return fragment{}, err
	}
	if len(body.entries) == 0 {
		return fragment{}, &CompileError{Category: ErrorInvalidPolicyLifetime, Source: policySource, Detail: "policy scope has an empty body"}
	}
	contents := make([]int, len(n.ir.Nodes)-firstNode)
	for index := range contents {
		contents[index] = firstNode + index
	}
	n.ir.Scopes = append(n.ir.Scopes, Scope{
		ID:     len(n.ir.Scopes),
		Policy: PolicyIR{Source: policySource, Name: policy.schema.Name, Arguments: append([]Argument(nil), policy.arguments...)},
		Body:   contents,
	})
	return body, nil
}

func normalizeAtomic(ir *IR, value instructionValue, source int) (Node, error) {
	if value.class == bindInstruction {
		if value.projection == nil {
			return Node{}, invalidInstruction(source, "binding has no projection")
		}
		if err := collectArguments(ir.Symbols, value.projection.Schema, value.projection.Arguments, source); err != nil {
			return Node{}, err
		}
		if err := collectSymbol(ir.Symbols, value.binding, value.projection.Schema.Output, source); err != nil {
			return Node{}, err
		}
		return Node{
			Source:    source,
			Kind:      BindingKind,
			Name:      value.projection.Schema.Name,
			Arguments: append([]Argument(nil), value.projection.Arguments...),
			Binding:   value.binding,
		}, nil
	}
	if value.class != atomicInstruction {
		return Node{}, invalidInstruction(source, "unknown instruction")
	}
	if err := collectArguments(ir.Symbols, value.schema, value.arguments, source); err != nil {
		return Node{}, err
	}
	return Node{
		Source:    source,
		Kind:      value.schema.Kind,
		Name:      value.schema.Name,
		Arguments: append([]Argument(nil), value.arguments...),
	}, nil
}

func (n *normalizer) connect(from, to []int) {
	for _, source := range from {
		for _, destination := range to {
			if source != destination {
				n.edges[Edge{From: source, To: destination}] = struct{}{}
			}
		}
	}
}

func (n *normalizer) applyBefore() error {
	for _, constraint := range n.before {
		from, fromOK := n.ir.Labels[constraint.from]
		to, toOK := n.ir.Labels[constraint.to]
		if !fromOK || !toOK {
			return invalidInstruction(constraint.source, fmt.Sprintf("Before references unknown labels %q and %q", constraint.from, constraint.to))
		}
		n.connect([]int{from}, []int{to})
	}
	return nil
}

func (n *normalizer) applyBindingEdges() error {
	producers := map[string]int{}
	for _, node := range n.ir.Nodes {
		if node.Binding == "" {
			continue
		}
		if previous, exists := producers[node.Binding]; exists {
			return &CompileError{
				Category: ErrorAmbiguousGrounding,
				Source:   node.Source,
				Related:  []int{n.ir.Nodes[previous].Source},
				Symbol:   node.Binding,
				Detail:   fmt.Sprintf("value symbol %q has multiple binding instructions", node.Binding),
			}
		}
		producers[node.Binding] = node.ID
	}
	for _, node := range n.ir.Nodes {
		for _, argument := range node.Arguments {
			producer, exists := producers[argument.SymbolName]
			if !exists || producer == node.ID {
				continue
			}
			n.connect([]int{producer}, []int{node.ID})
		}
	}
	for _, scope := range n.ir.Scopes {
		for _, argument := range scope.Policy.Arguments {
			producer, exists := producers[argument.SymbolName]
			if exists {
				n.connect([]int{producer}, scope.Body)
			}
		}
	}
	return nil
}

func hasCycle(nodes int, edges []Edge) bool {
	adjacent := make([][]int, nodes)
	indegree := make([]int, nodes)
	for _, edge := range edges {
		adjacent[edge.From] = append(adjacent[edge.From], edge.To)
		indegree[edge.To]++
	}
	queue := make([]int, 0, nodes)
	for node, degree := range indegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adjacent[node] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return visited != nodes
}

func collectArguments(symbols Symbols, schema Schema, arguments []Argument, source int) error {
	if len(arguments) != len(schema.Parameters) {
		return invalidInstruction(source, fmt.Sprintf("%s expects %d arguments, got %d", schema.Name, len(schema.Parameters), len(arguments)))
	}
	for index, parameter := range schema.Parameters {
		argument := arguments[index]
		switch parameter.Mode {
		case SymbolParameterMode:
			if argument.Literal || argument.SymbolName == "" {
				return invalidInstruction(source, fmt.Sprintf("%s.%s requires a symbol", schema.Name, parameter.Name))
			}
			if err := collectSymbol(symbols, argument.SymbolName, parameter.Type, source); err != nil {
				return err
			}
		case LiteralParameterMode:
			if !argument.Literal {
				return invalidInstruction(source, fmt.Sprintf("%s.%s requires a literal", schema.Name, parameter.Name))
			}
		default:
			return invalidInstruction(source, fmt.Sprintf("%s.%s has an invalid parameter mode", schema.Name, parameter.Name))
		}
	}
	return nil
}

func collectSymbol(symbols Symbols, name string, symbolType Type, source int) error {
	if name == "" {
		return invalidInstruction(source, "symbol name is empty")
	}
	previous, ok := symbols[name]
	if !ok {
		symbols[name] = SymbolInfo{Name: name, Type: symbolType, FirstSource: source, Uses: []int{source}}
		return nil
	}
	if previous.Type != symbolType {
		return &CompileError{
			Category: ErrorSymbolTypeConflict,
			Source:   source,
			Related:  []int{previous.FirstSource},
			Symbol:   name,
			Expected: symbolType.String(),
			Actual:   previous.Type.String(),
		}
	}
	previous.Uses = append(previous.Uses, source)
	symbols[name] = previous
	return nil
}

func invalidInstruction(source int, detail string) error {
	return &CompileError{Category: ErrorInvalidInstruction, Source: source, Detail: detail}
}
