package assurance

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/rule"
)

const unmigratedReason = "imperative runtime rule has not been migrated to the shared property algebra"

type declaration struct {
	safety   func() umpire.SafetyRule
	liveness func() umpire.LivenessRule
	included bool
	reason   string
}

type entry struct {
	name     string
	kind     string
	safety   func() umpire.SafetyRule
	liveness func() umpire.LivenessRule
	included bool
	reason   string
}

type Catalog struct {
	entries []entry
}

func Default() (*Catalog, error) {
	return compile([]declaration{
		// Safety rules — checked on every observation.
		{safety: func() umpire.SafetyRule { return &rule.SpeculativeTaskCreation{} }, reason: unmigratedReason},
		{safety: func() umpire.SafetyRule { return &rule.NexusOperationClosure{} }, reason: unmigratedReason},
		{safety: func() umpire.SafetyRule { return &rule.NexusActivityLinkConsistency{} }, included: true},
		{safety: func() umpire.SafetyRule { return &rule.NexusOperationTimeoutSemantics{} }, reason: unmigratedReason},
		{safety: func() umpire.SafetyRule { return &rule.CallbackReferenceConsistency{} }, reason: unmigratedReason},
		{safety: func() umpire.SafetyRule { return &rule.CallbackResponseConsistency{} }, reason: unmigratedReason},
		// Liveness rules — checked at test teardown.
		{liveness: func() umpire.LivenessRule { return &rule.WorkflowTaskStarvation{} }, reason: unmigratedReason},
		{liveness: func() umpire.LivenessRule { return &rule.EntityProgress{} }, included: true},
	})
}

func compile(declarations []declaration) (*Catalog, error) {
	result := &Catalog{entries: make([]entry, 0, len(declarations))}
	names := make(map[string]struct{}, len(declarations))
	for index, declared := range declarations {
		compiled := entry{included: declared.included, reason: declared.reason}
		switch {
		case declared.safety != nil && declared.liveness != nil:
			return nil, fmt.Errorf("assurance declaration %d has both safety and liveness factories", index)
		case declared.safety != nil:
			compiled.kind = "safety"
			compiled.safety = declared.safety
			compiled.name = strings.TrimSpace(declared.safety().Name())
		case declared.liveness != nil:
			compiled.kind = "liveness"
			compiled.liveness = declared.liveness
			compiled.name = strings.TrimSpace(declared.liveness().Name())
		default:
			return nil, fmt.Errorf("assurance declaration %d has no rule factory", index)
		}
		if compiled.name == "" {
			return nil, fmt.Errorf("assurance declaration %d has an empty rule name", index)
		}
		if _, duplicate := names[compiled.name]; duplicate {
			return nil, fmt.Errorf("duplicate assurance rule %q", compiled.name)
		}
		if !compiled.included && compiled.reason == "" {
			return nil, fmt.Errorf("excluded assurance rule %q requires a reason", compiled.name)
		}
		if compiled.included && compiled.reason != "" {
			return nil, fmt.Errorf("included assurance rule %q cannot have an exclusion reason", compiled.name)
		}
		names[compiled.name] = struct{}{}
		result.entries = append(result.entries, compiled)
	}
	slices.SortFunc(result.entries, func(left, right entry) int {
		return strings.Compare(left.name, right.name)
	})
	return result, nil
}

func (c *Catalog) Register(registry *umpire.RuleRegistry) error {
	if c == nil {
		return errors.New("assurance catalog is nil")
	}
	if registry == nil {
		return errors.New("assurance rule registry is nil")
	}
	for _, registered := range c.entries {
		if registered.safety != nil {
			registry.RegisterSafety(registered.safety)
		} else {
			registry.RegisterLiveness(registered.liveness)
		}
	}
	return nil
}

func (c *Catalog) Names() []string {
	if c == nil {
		return nil
	}
	result := make([]string, len(c.entries))
	for index, registered := range c.entries {
		result[index] = registered.name
	}
	return result
}

func (c *Catalog) CoveragePoints() []umpire.CoveragePoint {
	if c == nil {
		return nil
	}
	result := make([]umpire.CoveragePoint, len(c.entries))
	for index, registered := range c.entries {
		result[index] = umpire.CoveragePoint{Kind: umpire.CoverageRuleEvaluated, ID: registered.name}
	}
	return result
}

func (c *Catalog) VerificationInventory() []verify.InventoryItem {
	if c == nil {
		return nil
	}
	result := make([]verify.InventoryItem, len(c.entries))
	for index, registered := range c.entries {
		result[index] = verify.InventoryItem{
			Kind: "rule", Name: registered.name, Included: registered.included, Reason: registered.reason,
			Source: verify.Provenance{Path: "tests/umpire2/rule", Symbol: registered.name},
		}
	}
	return result
}
