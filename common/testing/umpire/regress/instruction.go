// Package regress provides typed sparse regression plans over an Umpire model catalog.
package regress

import "fmt"

// PathMode selects one canonical satisfying path or every satisfying semantic path.
type PathMode uint8

const (
	OnePathMode PathMode = iota
	AllPathsMode
)

// TypeClass distinguishes entity identities from derived model values.
type TypeClass uint8

const (
	EntityTypeClass TypeClass = iota + 1
	ValueTypeClass
)

// Type is the compile-time type of a symbolic or literal instruction argument.
type Type struct {
	Name  string    `json:"name"`
	Class TypeClass `json:"class"`
}

func EntityType(name string) Type { return Type{Name: name, Class: EntityTypeClass} }
func ValueType(name string) Type  { return Type{Name: name, Class: ValueTypeClass} }

func (t Type) String() string {
	prefix := "Value"
	if t.Class == EntityTypeClass {
		prefix = "Entity"
	}
	return fmt.Sprintf("%s<%s>", prefix, t.Name)
}

// ParameterMode controls whether a schema parameter accepts a symbol or a concrete literal.
type ParameterMode uint8

const (
	SymbolParameterMode ParameterMode = iota + 1
	LiteralParameterMode
)

// Parameter is one typed input in an instruction or projection schema.
type Parameter struct {
	Name string        `json:"name"`
	Type Type          `json:"type"`
	Mode ParameterMode `json:"mode"`
}

func SymbolParameter(name string, t Type) Parameter {
	return Parameter{Name: name, Type: t, Mode: SymbolParameterMode}
}

func LiteralParameter(name string, t Type) Parameter {
	return Parameter{Name: name, Type: t, Mode: LiteralParameterMode}
}

// InstructionKind identifies the semantic category of one key frame.
type InstructionKind uint8

const (
	OutcomeKind InstructionKind = iota + 1
	ActionKind
	RelationKind
	BindingKind
	PolicyKind
	RequirementKind
)

// Schema maps a typed authoring constructor to a registered model noun or verb.
type Schema struct {
	Name       string          `json:"name"`
	Kind       InstructionKind `json:"kind"`
	Parameters []Parameter     `json:"parameters,omitempty"`
	Output     Type            `json:"output,omitempty"`
}

func OutcomeSchema(name string, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: OutcomeKind, Parameters: parameters}
}

func ActionSchema(name string, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: ActionKind, Parameters: parameters}
}

func RelationSchema(name string, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: RelationKind, Parameters: parameters}
}

func PolicySchema(name string, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: PolicyKind, Parameters: parameters}
}

func RequirementSchema(name string, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: RequirementKind, Parameters: parameters}
}

func ProjectionSchema(name string, output Type, parameters ...Parameter) Schema {
	return Schema{Name: name, Kind: BindingKind, Parameters: parameters, Output: output}
}

// Argument is either a symbolic name or a concrete typed value supplied to an instruction.
type Argument struct {
	SymbolName string `json:"symbol,omitempty"`
	Value      any    `json:"value,omitempty"`
	Literal    bool   `json:"literal,omitempty"`
}

func Symbol(name string) Argument { return Argument{SymbolName: name} }
func Literal(value any) Argument  { return Argument{Value: value, Literal: true} }

type instructionClass uint8

const (
	atomicInstruction instructionClass = iota + 1
	bindInstruction
	anyOrderInstruction
	duringInstruction
	stepInstruction
	beforeInstruction
	repeatInstruction
)

// Instruction is a declarative key frame or composition directive.
type Instruction interface {
	instruction() instructionValue
}

type instructionValue struct {
	class       instructionClass
	schema      Schema
	arguments   []Argument
	binding     string
	projection  *Projection
	children    []Instruction
	policy      Instruction
	label       string
	before      string
	repetitions int
}

func (i instructionValue) instruction() instructionValue { return i }

// Projection names a typed value derived from model state.
type Projection struct {
	Schema    Schema
	Arguments []Argument
}

func Project(schema Schema, arguments ...Argument) Projection {
	return Projection{Schema: schema, Arguments: arguments}
}

func Outcome(schema Schema, arguments ...Argument) Instruction {
	return instructionValue{class: atomicInstruction, schema: schema, arguments: arguments}
}

func Action(schema Schema, arguments ...Argument) Instruction {
	return instructionValue{class: atomicInstruction, schema: schema, arguments: arguments}
}

func Relation(schema Schema, arguments ...Argument) Instruction {
	return instructionValue{class: atomicInstruction, schema: schema, arguments: arguments}
}

func Policy(schema Schema, arguments ...Argument) Instruction {
	return instructionValue{class: atomicInstruction, schema: schema, arguments: arguments}
}

func Require(schema Schema, arguments ...Argument) Instruction {
	return instructionValue{class: atomicInstruction, schema: schema, arguments: arguments}
}

func Bind(name string, projection Projection) Instruction {
	return instructionValue{class: bindInstruction, binding: name, projection: &projection}
}

// AnyOrder leaves its children mutually unordered while preserving their surrounding boundaries.
func AnyOrder(instructions ...Instruction) Instruction {
	return instructionValue{class: anyOrderInstruction, children: instructions}
}

// During scopes a registered policy over all behavior synthesized for its ordered body.
func During(policy Instruction, body ...Instruction) Instruction {
	return instructionValue{class: duringInstruction, policy: policy, children: body}
}

// Step labels one instruction occurrence for use by Before.
func Step(label string, instruction Instruction) Instruction {
	return instructionValue{class: stepInstruction, label: label, children: []Instruction{instruction}}
}

// Before adds a non-local ordering edge between two labeled instruction occurrences.
func Before(earlier, later string) Instruction {
	return instructionValue{class: beforeInstruction, label: earlier, before: later}
}

// Repeat requests an explicit finite number of occurrences of an instruction sequence.
func Repeat(count int, instructions ...Instruction) Instruction {
	return instructionValue{class: repeatInstruction, repetitions: count, children: instructions}
}

// Plan is the sparse source value compiled against a model domain and environment profile.
type Plan struct {
	Name         string
	Mode         PathMode
	instructions []Instruction
}

// Named assigns the stable diagnostic name recorded in completed artifacts.
func Named(name string, plan Plan) Plan {
	plan.Name = name
	return plan
}

func OnePath(instructions ...Instruction) Plan {
	return Plan{Mode: OnePathMode, instructions: instructions}
}

func AllPaths(instructions ...Instruction) Plan {
	return Plan{Mode: AllPathsMode, instructions: instructions}
}
