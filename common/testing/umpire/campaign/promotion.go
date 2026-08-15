package campaign

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

func semanticTrace(trace umpire.Trace) ([]string, error) {
	result := make([]string, 0, len(trace.Events))
	for _, event := range trace.Events {
		encoded, err := encodeTraceEvent(event)
		if err != nil {
			return nil, fmt.Errorf("%w: encode replay trace: %v", ErrInvalidRequest, err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

func encodeTraceEvent(event umpire.TraceEvent) (string, error) {
	causes := slices.Clone(event.Causes)
	slices.Sort(causes)
	encoded, err := json.Marshal(struct {
		Key            string                `json:"key"`
		Kind           umpire.TraceKind      `json:"kind"`
		Name           string                `json:"name"`
		Source         umpire.EvidenceSource `json:"source"`
		ClockDomain    string                `json:"clockDomain"`
		SourceSequence uint64                `json:"sourceSequence"`
		Causes         []string              `json:"causes,omitempty"`
		Fields         map[string]string     `json:"fields,omitempty"`
	}{
		Key: event.Key, Kind: event.Kind, Name: event.Name, Source: event.Source,
		ClockDomain: event.ClockDomain, SourceSequence: event.SourceSequence,
		Causes: causes, Fields: event.Fields,
	})
	return string(encoded), err
}

func promote(request Request, discovery Discovery) (RegressionCandidate, error) {
	stableScenario := cloneScenario(discovery.Minimized)
	stableScenario.Path.Bindings = nil
	candidate := RegressionCandidate{
		Name:         discovery.Minimized.Name,
		ModelVersion: discovery.Minimized.ModelVersion,
		Environment:  request.Environment,
		Property:     discovery.MinimizedRun.Claim.Property,
		SparseIR:     sparseIR(request.Template.IR, stableScenario.Path),
		Scenario:     stableScenario,
		ReplayCommands: compactCommands([][]string{
			discovery.MinimizedRun.ReplayCommand,
			discovery.Replay.ReplayCommand,
		}),
	}
	scenarioKey, err := ScenarioKey(stableScenario)
	if err != nil {
		return RegressionCandidate{}, err
	}
	payload, err := json.Marshal(struct {
		ScenarioKey string     `json:"scenarioKey"`
		Property    string     `json:"property"`
		SparseIR    regress.IR `json:"sparseIR"`
		Environment string     `json:"environment"`
	}{ScenarioKey: scenarioKey, Property: candidate.Property, SparseIR: candidate.SparseIR, Environment: candidate.Environment.Name})
	if err != nil {
		return RegressionCandidate{}, fmt.Errorf("%w: encode regression candidate: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(payload)
	candidate.ID = hex.EncodeToString(digest[:])
	return candidate, nil
}

func sparseIR(template regress.IR, path regress.CompletedPath) regress.IR {
	result := regress.IR{Mode: regress.OnePathMode, Symbols: cloneSymbols(template.Symbols)}
	actionNodes := make([]int, len(path.Actions))
	appendMilestones := func(boundary int) {
		for _, milestone := range path.Milestones {
			if milestone.AfterAction != boundary {
				continue
			}
			result.Nodes = append(result.Nodes, regress.Node{
				ID:        len(result.Nodes),
				Source:    milestone.Source,
				Kind:      milestone.Kind,
				Name:      milestone.Name,
				Arguments: slices.Clone(milestone.Arguments),
				Binding:   milestone.Binding,
			})
		}
	}
	appendMilestones(0)
	for index, action := range path.Actions {
		nodeID := len(result.Nodes)
		actionNodes[index] = nodeID
		result.Nodes = append(result.Nodes, regress.Node{
			ID:        nodeID,
			Source:    action.Source,
			Kind:      regress.ActionKind,
			Name:      action.Name,
			Arguments: slices.Clone(action.Arguments),
		})
		appendMilestones(index + 1)
	}
	for index := 1; index < len(result.Nodes); index++ {
		result.Edges = append(result.Edges, regress.Edge{From: result.Nodes[index-1].ID, To: result.Nodes[index].ID})
	}
	for index, policy := range path.Policies {
		body := slices.Clone(actionNodes[policy.Start:policy.End])
		result.Scopes = append(result.Scopes, regress.Scope{
			ID: index,
			Policy: regress.PolicyIR{
				Source:    policy.Source,
				Name:      policy.Name,
				Arguments: slices.Clone(policy.Arguments),
			},
			Body: body,
		})
	}
	result.Requirements = slices.Clone(template.Requirements)
	for index := range result.Requirements {
		result.Requirements[index].Arguments = slices.Clone(template.Requirements[index].Arguments)
	}
	return result
}

func cloneSymbols(symbols regress.Symbols) regress.Symbols {
	result := make(regress.Symbols, len(symbols))
	for name, symbol := range symbols {
		symbol.Uses = slices.Clone(symbol.Uses)
		result[name] = symbol
	}
	return result
}

func compactCommands(commands [][]string) [][]string {
	var result [][]string
	for _, command := range commands {
		if len(command) == 0 {
			continue
		}
		if len(result) == 0 || !slices.Equal(result[len(result)-1], command) {
			result = append(result, slices.Clone(command))
		}
	}
	return result
}
