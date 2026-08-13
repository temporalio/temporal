package protocol

import (
	"errors"
	"fmt"
	"slices"

	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
)

// NamedCausalFootprint gives one checked-in action footprint a stable catalog name.
type NamedCausalFootprint struct {
	Name      string
	Footprint umpirefw.CausalFootprint
}

// DefaultCausalFootprints returns the validated Temporal action-footprint catalog.
func DefaultCausalFootprints() ([]NamedCausalFootprint, error) {
	declarations := []NamedCausalFootprint{
		{
			Name: "ordinary-completion",
			Footprint: factFootprint("nexus.respond_start.scheduled.sync",
				"NexusOperationTerminal"),
		},
		{
			Name: "completion-before-start",
			Footprint: factFootprint("nexus.complete.scheduled",
				"NexusCallbackObservation", "NexusOperationTerminal"),
		},
		{
			Name: "cancellation-failure-then-cancellation",
			Footprint: factFootprint("nexus.cancel_with_retry",
				"NexusOperationCancelRequestFailed", "NexusOperationTerminal"),
		},
		{
			Name: "shared-handler-attachment",
			Footprint: umpirefw.CausalFootprint{
				Action: "nexus.start.attach_handler",
				Refinement: umpirefw.TraceRefinement{
					Required: []umpirefw.TracePattern{
						{Kind: umpirefw.TraceFact, Name: "NexusCallbackObservation"},
						{Kind: umpirefw.TraceFact, Name: "WorkflowCallbackAttachment"},
						{Kind: umpirefw.TraceRelation, Name: string(model.CallbackOperationRelation)},
						{Kind: umpirefw.TraceRelation, Name: string(model.CallbackHandlerRunRelation)},
					},
					AllowExtras: true,
				},
				Causal: []umpirefw.TracePattern{
					{Kind: umpirefw.TraceFact, Name: "NexusCallbackObservation"},
					{Kind: umpirefw.TraceFact, Name: "WorkflowCallbackAttachment"},
				},
			},
		},
	}
	return CompileCausalFootprints([]string{
		"nexus.respond_start.scheduled.sync",
		"nexus.complete.scheduled",
		"nexus.cancel_with_retry",
		"nexus.start.attach_handler",
	}, declarations)
}

func factFootprint(action string, names ...string) umpirefw.CausalFootprint {
	patterns := make([]umpirefw.TracePattern, len(names))
	for index, name := range names {
		patterns[index] = umpirefw.TracePattern{Kind: umpirefw.TraceFact, Name: name}
	}
	return umpirefw.CausalFootprint{
		Action: action,
		Refinement: umpirefw.TraceRefinement{
			Required:    slices.Clone(patterns),
			AllowExtras: true,
		},
		Causal: patterns,
	}
}

// CompileCausalFootprints validates names, action ownership, and semantic trace patterns.
func CompileCausalFootprints(knownActions []string, declarations []NamedCausalFootprint) ([]NamedCausalFootprint, error) {
	actions := make(map[string]struct{}, len(knownActions))
	for _, action := range knownActions {
		if action == "" {
			return nil, errors.New("causal footprints: known action is empty")
		}
		actions[action] = struct{}{}
	}
	patterns := defaultTracePatterns()
	names := map[string]struct{}{}
	ownedActions := map[string]struct{}{}
	result := make([]NamedCausalFootprint, 0, len(declarations))
	for _, declaration := range declarations {
		if declaration.Name == "" {
			return nil, errors.New("causal footprints: name is empty")
		}
		if _, duplicate := names[declaration.Name]; duplicate {
			return nil, fmt.Errorf("causal footprints: duplicate name %q", declaration.Name)
		}
		names[declaration.Name] = struct{}{}
		action := declaration.Footprint.Action
		if _, known := actions[action]; !known {
			return nil, fmt.Errorf("causal footprints: %q references unknown action %q", declaration.Name, action)
		}
		if _, duplicate := ownedActions[action]; duplicate {
			return nil, fmt.Errorf("causal footprints: duplicate action %q", action)
		}
		ownedActions[action] = struct{}{}
		for _, pattern := range append(append(slices.Clone(declaration.Footprint.Refinement.Required), declaration.Footprint.Refinement.Forbidden...), declaration.Footprint.Causal...) {
			if _, known := patterns[pattern]; !known {
				return nil, fmt.Errorf("causal footprints: %q references unknown pattern %s/%s", declaration.Name, pattern.Kind, pattern.Name)
			}
		}
		result = append(result, cloneNamedCausalFootprint(declaration))
	}
	return result, nil
}

func defaultTracePatterns() map[umpirefw.TracePattern]struct{} {
	patterns := map[umpirefw.TracePattern]struct{}{}
	for _, observed := range model.DefaultFacts() {
		patterns[umpirefw.TracePattern{Kind: umpirefw.TraceFact, Name: observed.Name()}] = struct{}{}
	}
	for _, relation := range defaultRelationSchemas() {
		patterns[umpirefw.TracePattern{Kind: umpirefw.TraceRelation, Name: string(relation.Type)}] = struct{}{}
	}
	return patterns
}

func cloneNamedCausalFootprint(source NamedCausalFootprint) NamedCausalFootprint {
	source.Footprint.Refinement.Required = slices.Clone(source.Footprint.Refinement.Required)
	source.Footprint.Refinement.Forbidden = slices.Clone(source.Footprint.Refinement.Forbidden)
	source.Footprint.Causal = slices.Clone(source.Footprint.Causal)
	return source
}
