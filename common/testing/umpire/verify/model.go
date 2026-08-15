package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
)

type Cardinality string

const (
	One  Cardinality = "one"
	Many Cardinality = "many"
)

type BindingMode string

const (
	InputBinding    BindingMode = "input"
	FreshBinding    BindingMode = "fresh"
	ObservedBinding BindingMode = "observed"
)

type EffectKind string

const (
	CreateEffect         EffectKind = "create"
	SetStateEffect       EffectKind = "set-state"
	AddRelationEffect    EffectKind = "add-relation"
	RemoveRelationEffect EffectKind = "remove-relation"
)

type PropertyKind string

const (
	SafetyProperty    PropertyKind = "safety"
	QuiescentProperty PropertyKind = "quiescent"
	ProgressProperty  PropertyKind = "progress"
)

type ExprOp string

const (
	TrueExpr         ExprOp = "true"
	FalseExpr        ExprOp = "false"
	NotExpr          ExprOp = "not"
	AndExpr          ExprOp = "and"
	OrExpr           ExprOp = "or"
	ImpliesExpr      ExprOp = "implies"
	EntityExistsExpr ExprOp = "entity-exists"
	StateIsExpr      ExprOp = "state-is"
	RelationHasExpr  ExprOp = "relation-has"
	ForAllExpr       ExprOp = "for-all"
	ExistsExpr       ExprOp = "exists"
)

type Provenance struct {
	Path   string `json:"path,omitempty"`
	Symbol string `json:"symbol,omitempty"`
}

type State struct {
	Name         string `json:"name"`
	Terminal     bool   `json:"terminal,omitempty"`
	MustProgress bool   `json:"mustProgress,omitempty"`
	Disposition  string `json:"disposition,omitempty"`
}

type EntityType struct {
	Name            string     `json:"name"`
	IDs             []string   `json:"ids"`
	InitiallyExists []string   `json:"initiallyExists,omitempty"`
	Initial         string     `json:"initial"`
	States          []State    `json:"states"`
	Source          Provenance `json:"source,omitempty"`
}

type Relation struct {
	Name              string      `json:"name"`
	Source            string      `json:"source"`
	Target            string      `json:"target"`
	SourceCardinality Cardinality `json:"sourceCardinality"`
	TargetCardinality Cardinality `json:"targetCardinality"`
	SourceLocation    Provenance  `json:"sourceLocation,omitempty"`
}

type Parameter struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Binding BindingMode `json:"binding"`
}

func DistinctFreshParameterPairs(parameters []Parameter) [][2]Parameter {
	var result [][2]Parameter
	for left := range parameters {
		if parameters[left].Binding != FreshBinding {
			continue
		}
		for right := left + 1; right < len(parameters); right++ {
			if parameters[right].Binding == FreshBinding && parameters[right].Type == parameters[left].Type {
				result = append(result, [2]Parameter{parameters[left], parameters[right]})
			}
		}
	}
	return result
}

type Expr struct {
	Op       ExprOp `json:"op,omitempty"`
	Args     []Expr `json:"args,omitempty"`
	Entity   string `json:"entity,omitempty"`
	Ref      string `json:"ref,omitempty"`
	State    string `json:"state,omitempty"`
	Relation string `json:"relation,omitempty"`
	Source   string `json:"source,omitempty"`
	Target   string `json:"target,omitempty"`
	Var      string `json:"var,omitempty"`
}

func StateIs(entity, ref, state string) Expr {
	return Expr{Op: StateIsExpr, Entity: entity, Ref: ref, State: state}
}

func Not(argument Expr) Expr {
	return Expr{Op: NotExpr, Args: []Expr{argument}}
}

func And(arguments ...Expr) Expr {
	if len(arguments) == 1 {
		return arguments[0]
	}
	return Expr{Op: AndExpr, Args: arguments}
}

type Effect struct {
	Kind     EffectKind `json:"kind"`
	Entity   string     `json:"entity,omitempty"`
	Ref      string     `json:"ref,omitempty"`
	State    string     `json:"state,omitempty"`
	Relation string     `json:"relation,omitempty"`
	Source   string     `json:"source,omitempty"`
	Target   string     `json:"target,omitempty"`
}

type Branch struct {
	Name    string   `json:"name"`
	Effects []Effect `json:"effects,omitempty"`
}

type Action struct {
	Name         string      `json:"name"`
	Parameters   []Parameter `json:"parameters,omitempty"`
	Guard        Expr        `json:"guard,omitempty"`
	Effects      []Effect    `json:"effects,omitempty"`
	Branches     []Branch    `json:"branches,omitempty"`
	Hosting      string      `json:"hosting,omitempty"`
	Capabilities []string    `json:"capabilities,omitempty"`
	Unrealized   bool        `json:"unrealized,omitempty"`
	Source       Provenance  `json:"source,omitempty"`
}

type Property struct {
	Name          string       `json:"name"`
	Kind          PropertyKind `json:"kind"`
	Expr          Expr         `json:"expr"`
	Fairness      []string     `json:"fairness,omitempty"`
	Strengthening bool         `json:"strengthening,omitempty"`
	Source        Provenance   `json:"source,omitempty"`
}

type Abstraction struct {
	Name   string     `json:"name"`
	Reason string     `json:"reason"`
	Source Provenance `json:"source,omitempty"`
}

type InventoryItem struct {
	Kind     string     `json:"kind"`
	Name     string     `json:"name"`
	Included bool       `json:"included,omitempty"`
	Reason   string     `json:"reason,omitempty"`
	Source   Provenance `json:"source,omitempty"`
}

type Refinement struct {
	Name                  string     `json:"name"`
	Action                string     `json:"action,omitempty"`
	LifecycleActions      []string   `json:"lifecycleActions,omitempty"`
	RegressionActions     []string   `json:"regressionActions,omitempty"`
	RequiredObservations  []string   `json:"requiredObservations,omitempty"`
	ForbiddenObservations []string   `json:"forbiddenObservations,omitempty"`
	Stutter               bool       `json:"stutter,omitempty"`
	Source                Provenance `json:"source,omitempty"`
}

type Model struct {
	Version      string          `json:"version"`
	Entities     []EntityType    `json:"entities,omitempty"`
	Relations    []Relation      `json:"relations,omitempty"`
	Actions      []Action        `json:"actions,omitempty"`
	Properties   []Property      `json:"properties,omitempty"`
	Abstractions []Abstraction   `json:"abstractions,omitempty"`
	Inventory    []InventoryItem `json:"inventory,omitempty"`
	Refinements  []Refinement    `json:"refinements,omitempty"`
}

func MarshalModel(model Model) ([]byte, error) {
	normalized := normalizeModel(model)
	encoded, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func HashModel(model Model) (string, error) {
	encoded, err := MarshalModel(model)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func normalizeModel(model Model) Model {
	result := model
	result.Entities = slices.Clone(model.Entities)
	for index := range result.Entities {
		result.Entities[index].IDs = sortedClone(result.Entities[index].IDs)
		result.Entities[index].InitiallyExists = sortedClone(result.Entities[index].InitiallyExists)
		result.Entities[index].States = slices.Clone(result.Entities[index].States)
		slices.SortFunc(result.Entities[index].States, func(left, right State) int {
			return compareString(left.Name, right.Name)
		})
	}
	slices.SortFunc(result.Entities, func(left, right EntityType) int {
		return compareString(left.Name, right.Name)
	})
	result.Relations = slices.Clone(model.Relations)
	slices.SortFunc(result.Relations, func(left, right Relation) int {
		return compareString(left.Name, right.Name)
	})
	result.Actions = slices.Clone(model.Actions)
	for index := range result.Actions {
		result.Actions[index].Parameters = slices.Clone(result.Actions[index].Parameters)
		result.Actions[index].Effects = slices.Clone(result.Actions[index].Effects)
		result.Actions[index].Branches = slices.Clone(result.Actions[index].Branches)
		result.Actions[index].Capabilities = sortedClone(result.Actions[index].Capabilities)
	}
	slices.SortFunc(result.Actions, func(left, right Action) int {
		return compareString(left.Name, right.Name)
	})
	result.Properties = slices.Clone(model.Properties)
	for index := range result.Properties {
		result.Properties[index].Fairness = sortedClone(result.Properties[index].Fairness)
	}
	slices.SortFunc(result.Properties, func(left, right Property) int {
		return compareString(left.Name, right.Name)
	})
	result.Abstractions = slices.Clone(model.Abstractions)
	slices.SortFunc(result.Abstractions, func(left, right Abstraction) int {
		return compareString(left.Name, right.Name)
	})
	result.Inventory = slices.Clone(model.Inventory)
	slices.SortFunc(result.Inventory, func(left, right InventoryItem) int {
		if comparison := compareString(left.Kind, right.Kind); comparison != 0 {
			return comparison
		}
		return compareString(left.Name, right.Name)
	})
	result.Refinements = slices.Clone(model.Refinements)
	for index := range result.Refinements {
		result.Refinements[index].LifecycleActions = sortedClone(result.Refinements[index].LifecycleActions)
		result.Refinements[index].RegressionActions = sortedClone(result.Refinements[index].RegressionActions)
		result.Refinements[index].RequiredObservations = sortedClone(result.Refinements[index].RequiredObservations)
		result.Refinements[index].ForbiddenObservations = sortedClone(result.Refinements[index].ForbiddenObservations)
	}
	slices.SortFunc(result.Refinements, func(left, right Refinement) int {
		return compareString(left.Name, right.Name)
	})
	return result
}

func sortedClone(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return result
}

func compareString(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
