package campaign

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

// ScenarioKey returns an environment-independent identity for completed semantic intent.
func ScenarioKey(scenario Scenario) (string, error) {
	type semanticAction struct {
		Name      string             `json:"name"`
		Arguments []regress.Argument `json:"arguments,omitempty"`
	}
	type semanticPolicy struct {
		Name      string             `json:"name"`
		Arguments []regress.Argument `json:"arguments,omitempty"`
		Start     int                `json:"start"`
		End       int                `json:"end"`
	}
	type semanticMilestone struct {
		Kind         regress.InstructionKind `json:"kind"`
		Name         string                  `json:"name"`
		Arguments    []regress.Argument      `json:"arguments,omitempty"`
		Binding      string                  `json:"binding,omitempty"`
		BeforeAction int                     `json:"beforeAction"`
		AfterAction  int                     `json:"afterAction"`
	}
	value := struct {
		ModelVersion     string              `json:"modelVersion"`
		Actions          []semanticAction    `json:"actions"`
		Policies         []semanticPolicy    `json:"policies,omitempty"`
		Milestones       []semanticMilestone `json:"milestones,omitempty"`
		Resources        []string            `json:"resources,omitempty"`
		Matrix           umpire.MatrixCase   `json:"matrix,omitempty"`
		ExplorationRoute []string            `json:"explorationRoute,omitempty"`
		Faults           []string            `json:"faults,omitempty"`
	}{
		ModelVersion:     scenario.ModelVersion,
		Matrix:           scenario.Matrix,
		ExplorationRoute: slices.Clone(scenario.ExplorationRoute),
		Faults:           slices.Clone(scenario.Faults),
	}
	for _, action := range scenario.Path.Actions {
		value.Actions = append(value.Actions, semanticAction{Name: action.Name, Arguments: slices.Clone(action.Arguments)})
	}
	for _, policy := range scenario.Path.Policies {
		value.Policies = append(value.Policies, semanticPolicy{Name: policy.Name, Arguments: slices.Clone(policy.Arguments), Start: policy.Start, End: policy.End})
	}
	for _, milestone := range scenario.Path.Milestones {
		value.Milestones = append(value.Milestones, semanticMilestone{
			Kind:         milestone.Kind,
			Name:         milestone.Name,
			Arguments:    slices.Clone(milestone.Arguments),
			Binding:      milestone.Binding,
			BeforeAction: milestone.BeforeAction,
			AfterAction:  milestone.AfterAction,
		})
	}
	for _, resource := range scenario.Path.Resources {
		value.Resources = append(value.Resources, resource.Name)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("%w: encode semantic scenario: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func scenarioSemantics(scenario Scenario) []string {
	var result []string
	for _, action := range scenario.Path.Actions {
		result = append(result, action.Name)
	}
	for _, policy := range scenario.Path.Policies {
		result = append(result, policy.Name)
	}
	for _, resource := range scenario.Path.Resources {
		result = append(result, resource.Name)
	}
	result = append(result, scenario.Faults...)
	return result
}

func scenarioCoverage(scenario Scenario) []umpire.CoveragePoint {
	var result []umpire.CoveragePoint
	for _, action := range scenario.Path.Actions {
		result = append(result, umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: action.Name})
	}
	for _, milestone := range scenario.Path.Milestones {
		kind := umpire.CoverageFact
		if milestone.Kind == regress.RelationKind {
			kind = umpire.CoverageRelation
		}
		result = append(result, umpire.CoveragePoint{Kind: kind, ID: milestone.Name})
	}
	return result
}

func coverageDifference(before, after []umpire.CoveragePoint) []umpire.CoveragePoint {
	seen := make(map[umpire.CoveragePoint]struct{}, len(before))
	for _, point := range before {
		seen[point] = struct{}{}
	}
	var result []umpire.CoveragePoint
	for _, point := range after {
		if _, exists := seen[point]; !exists {
			result = append(result, point)
		}
	}
	return result
}

func cloneScenario(scenario Scenario) Scenario {
	scenario.Path = clonePath(scenario.Path)
	scenario.Matrix = umpire.MatrixCase{Values: slices.Clone(scenario.Matrix.Values)}
	scenario.ExplorationRoute = slices.Clone(scenario.ExplorationRoute)
	scenario.Faults = slices.Clone(scenario.Faults)
	return scenario
}

func clonePath(path regress.CompletedPath) regress.CompletedPath {
	result := path
	result.Actions = make([]regress.CompletedAction, len(path.Actions))
	for index, action := range path.Actions {
		result.Actions[index] = action
		result.Actions[index].Arguments = slices.Clone(action.Arguments)
	}
	result.Steps = make([]regress.CompletedStep, len(path.Steps))
	for index, step := range path.Steps {
		result.Steps[index] = step
		result.Steps[index].Action.Arguments = slices.Clone(step.Action.Arguments)
		result.Steps[index].Preconditions = cloneAtoms(step.Preconditions)
		result.Steps[index].Effects = cloneAtoms(step.Effects)
	}
	result.Created = slices.Clone(path.Created)
	result.Resources = slices.Clone(path.Resources)
	result.Policies = make([]regress.CompletedPolicy, len(path.Policies))
	for index, policy := range path.Policies {
		result.Policies[index] = policy
		result.Policies[index].Arguments = slices.Clone(policy.Arguments)
	}
	result.Milestones = make([]regress.CompletedMilestone, len(path.Milestones))
	for index, milestone := range path.Milestones {
		result.Milestones[index] = milestone
		result.Milestones[index].Arguments = slices.Clone(milestone.Arguments)
	}
	result.Bindings = make(regress.Bindings, len(path.Bindings))
	for name, value := range path.Bindings {
		result.Bindings[name] = value
	}
	return result
}

func cloneAtoms(atoms []regress.CompletedAtom) []regress.CompletedAtom {
	result := make([]regress.CompletedAtom, len(atoms))
	for index, atom := range atoms {
		result[index] = atom
		result[index].Arguments = slices.Clone(atom.Arguments)
	}
	return result
}

func containsKey(keys map[string]struct{}, key string) bool {
	_, exists := keys[key]
	return exists
}

// Corpus is a bounded concurrency-safe set of canonical semantic scenario keys.
type Corpus struct {
	maxEntries int
	mu         sync.RWMutex
	keys       map[string]struct{}
}

// NewCorpus creates a bounded retained campaign corpus.
func NewCorpus(maxEntries int) *Corpus {
	return &Corpus{maxEntries: maxEntries, keys: map[string]struct{}{}}
}

// Contains reports whether equivalent semantic intent has already executed.
func (c *Corpus) Contains(key string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.keys[key]
	return exists
}

// Add retains a key unless the corpus is full.
func (c *Corpus) Add(key string) bool {
	if c == nil || key == "" || c.maxEntries < 1 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.keys[key]; exists {
		return false
	}
	if len(c.keys) >= c.maxEntries {
		return false
	}
	c.keys[key] = struct{}{}
	return true
}

// Keys returns the retained semantic keys in stable order.
func (c *Corpus) Keys() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, 0, len(c.keys))
	for key := range c.keys {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}
