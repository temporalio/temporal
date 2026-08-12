package regress

import (
	"errors"
	"fmt"
	"slices"
)

// BindingMode describes how an action capability grounds one of its variables.
type BindingMode uint8

const (
	InputBinding BindingMode = iota
	FreshBinding
	ObservedBinding
)

// ActionMode controls whether the executor fires an action or installs it to react to traffic.
type ActionMode uint8

const (
	ProactiveAction ActionMode = iota
	ReactiveAction
	ObservationAction
)

// Variable is one typed logical variable used by an action capability.
type Variable struct {
	Name    string
	Type    Type
	Binding BindingMode
}

// TemplateTerm is a capability-local variable or concrete literal in a predicate atom.
type TemplateTerm struct {
	Variable string
	Value    any
	Literal  bool
}

func TemplateVar(name string) TemplateTerm   { return TemplateTerm{Variable: name} }
func TemplateLiteral(value any) TemplateTerm { return TemplateTerm{Value: value, Literal: true} }

// AtomTemplate is a predicate over capability-local variables.
type AtomTemplate struct {
	Predicate string
	Terms     []TemplateTerm
}

func Atom(predicate string, terms ...TemplateTerm) AtomTemplate {
	return AtomTemplate{Predicate: predicate, Terms: terms}
}

// PredicateCapability describes a modeled outcome or relation. ExclusiveBy identifies the
// argument positions that form one mutable slot; applying a new value replaces the prior fact.
type PredicateCapability struct {
	Schema      Schema
	ExclusiveBy []int
}

// ActionCapability is a model-only causal operator used for synthesis and live realization.
type ActionCapability struct {
	Schema        Schema
	Mode          ActionMode
	Variables     []Variable
	Preconditions []AtomTemplate
	Effects       []AtomTemplate
	Resources     []string
	Requires      []string
	IndependentOf []string
	Fixed         map[string]any
	Realization   string
	catalogOrder  int
}

// ResourceCapability is a synthesized environment resource and its dependency lifetime.
type ResourceCapability struct {
	Name        string
	DependsOn   []string
	Requires    []string
	Realization string
}

// PolicyCapability is a registered scoped environment behavior.
type PolicyCapability struct {
	Schema      Schema
	Resources   []string
	Requires    []string
	Realization string
}

// Domain is the canonical immutable-at-compilation catalog of model capabilities.
type Domain struct {
	version    string
	predicates map[string]PredicateCapability
	actions    []ActionCapability
	resources  map[string]ResourceCapability
	policies   map[string]PolicyCapability
}

func NewDomain(version string) *Domain {
	return &Domain{
		version:    version,
		predicates: map[string]PredicateCapability{},
		resources:  map[string]ResourceCapability{},
		policies:   map[string]PolicyCapability{},
	}
}

func (d *Domain) Version() string { return d.version }

// Clone returns an independently mutable copy of the compiled catalog declaration.
func (d *Domain) Clone() *Domain {
	if d == nil {
		return nil
	}
	result := NewDomain(d.version)
	for name, predicate := range d.predicates {
		predicate.ExclusiveBy = slices.Clone(predicate.ExclusiveBy)
		result.predicates[name] = predicate
	}
	for _, action := range d.actions {
		action.Variables = slices.Clone(action.Variables)
		action.Preconditions = slices.Clone(action.Preconditions)
		action.Effects = slices.Clone(action.Effects)
		action.Resources = slices.Clone(action.Resources)
		action.Requires = slices.Clone(action.Requires)
		action.IndependentOf = slices.Clone(action.IndependentOf)
		action.Fixed = cloneMap(action.Fixed)
		result.actions = append(result.actions, action)
	}
	for name, resource := range d.resources {
		resource.DependsOn = slices.Clone(resource.DependsOn)
		resource.Requires = slices.Clone(resource.Requires)
		result.resources[name] = resource
	}
	for name, policy := range d.policies {
		policy.Resources = slices.Clone(policy.Resources)
		policy.Requires = slices.Clone(policy.Requires)
		result.policies[name] = policy
	}
	return result
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	if source == nil {
		return nil
	}
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (d *Domain) AddPredicate(predicate PredicateCapability) error {
	if predicate.Schema.Name == "" || (predicate.Schema.Kind != OutcomeKind && predicate.Schema.Kind != RelationKind && predicate.Schema.Kind != BindingKind) {
		return errors.New("predicate schema must be an outcome, relation, or binding")
	}
	if _, exists := d.predicates[predicate.Schema.Name]; exists {
		return fmt.Errorf("predicate %q already registered", predicate.Schema.Name)
	}
	for _, index := range predicate.ExclusiveBy {
		if index < 0 || index >= len(predicate.Schema.Parameters) {
			return fmt.Errorf("predicate %q has invalid exclusive argument %d", predicate.Schema.Name, index)
		}
	}
	d.predicates[predicate.Schema.Name] = predicate
	return nil
}

func (d *Domain) AddAction(action ActionCapability) error {
	if action.Schema.Name == "" || action.Schema.Kind != ActionKind {
		return errors.New("action capability requires an action schema")
	}
	variables := make(map[string]Type, len(action.Variables))
	for _, variable := range action.Variables {
		if variable.Name == "" || variable.Type.Name == "" {
			return fmt.Errorf("action %q has an invalid variable", action.Schema.Name)
		}
		if _, exists := variables[variable.Name]; exists {
			return fmt.Errorf("action %q repeats variable %q", action.Schema.Name, variable.Name)
		}
		variables[variable.Name] = variable.Type
	}
	for _, parameter := range action.Schema.Parameters {
		variableType, exists := variables[parameter.Name]
		if !exists || variableType != parameter.Type {
			return fmt.Errorf("action %q parameter %q has no matching variable", action.Schema.Name, parameter.Name)
		}
	}
	for name := range action.Fixed {
		if _, exists := variables[name]; !exists {
			return fmt.Errorf("action %q fixes unknown variable %q", action.Schema.Name, name)
		}
		if _, err := stableLiteralKey(action.Fixed[name]); err != nil {
			return fmt.Errorf("action %q fixed value %q has no stable encoding: %w", action.Schema.Name, name, err)
		}
	}
	for _, atom := range append(slices.Clone(action.Preconditions), action.Effects...) {
		predicate, exists := d.predicates[atom.Predicate]
		if !exists {
			return fmt.Errorf("action %q references unknown predicate %q", action.Schema.Name, atom.Predicate)
		}
		expectedTerms := predicateArity(predicate.Schema)
		if len(atom.Terms) != expectedTerms {
			return fmt.Errorf("action %q predicate %q expects %d terms, got %d", action.Schema.Name, atom.Predicate, expectedTerms, len(atom.Terms))
		}
		for index, term := range atom.Terms {
			if term.Literal {
				if _, err := stableLiteralKey(term.Value); err != nil {
					return fmt.Errorf("action %q predicate %q term %d has no stable encoding: %w", action.Schema.Name, atom.Predicate, index, err)
				}
				continue
			}
			variableType, exists := variables[term.Variable]
			if !exists {
				return fmt.Errorf("action %q references unknown variable %q", action.Schema.Name, term.Variable)
			}
			expectedType := predicateTermType(predicate.Schema, index)
			if variableType != expectedType {
				return fmt.Errorf("action %q uses variable %q of type %s for %s term %d of type %s", action.Schema.Name, term.Variable, variableType, atom.Predicate, index, expectedType)
			}
		}
	}
	action.catalogOrder = len(d.actions)
	d.actions = append(d.actions, action)
	return nil
}

func predicateTermType(schema Schema, index int) Type {
	if index < len(schema.Parameters) {
		return schema.Parameters[index].Type
	}
	return schema.Output
}

func predicateArity(schema Schema) int {
	result := len(schema.Parameters)
	if schema.Kind == BindingKind {
		result++
	}
	return result
}

func (d *Domain) AddResource(resource ResourceCapability) error {
	if resource.Name == "" {
		return errors.New("resource name is empty")
	}
	if _, exists := d.resources[resource.Name]; exists {
		return fmt.Errorf("resource %q already registered", resource.Name)
	}
	d.resources[resource.Name] = resource
	return nil
}

func (d *Domain) AddPolicy(policy PolicyCapability) error {
	if policy.Schema.Name == "" || policy.Schema.Kind != PolicyKind {
		return errors.New("policy capability requires a policy schema")
	}
	if _, exists := d.policies[policy.Schema.Name]; exists {
		return fmt.Errorf("policy %q already registered", policy.Schema.Name)
	}
	d.policies[policy.Schema.Name] = policy
	return nil
}
