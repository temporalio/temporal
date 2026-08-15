package regress

import "fmt"

// ActionRealization identifies one executable action and its required execution mode.
type ActionRealization struct {
	Name string     `json:"name"`
	Mode ActionMode `json:"mode"`
}

// RealizationCatalog declares the executable capabilities supplied by an environment.
type RealizationCatalog struct {
	Actions   []ActionRealization `json:"actions,omitempty"`
	Policies  []string            `json:"policies,omitempty"`
	Resources []string            `json:"resources,omitempty"`
}

// ValidateRealizations checks that every completed action has an executable realization.
func ValidateRealizations(suite Suite, catalog RealizationCatalog) error {
	if err := validateRealizationCatalog(catalog); err != nil {
		return err
	}
	actions := make(map[string]ActionMode, len(catalog.Actions))
	for _, action := range catalog.Actions {
		actions[action.Name] = action.Mode
	}
	resources := make(map[string]struct{}, len(catalog.Resources))
	for _, resource := range catalog.Resources {
		resources[resource] = struct{}{}
	}
	policies := make(map[string]struct{}, len(catalog.Policies))
	for _, policy := range catalog.Policies {
		policies[policy] = struct{}{}
	}
	for _, path := range suite.Paths {
		steps := path.Steps
		if len(steps) == 0 {
			steps = make([]CompletedStep, len(path.Actions))
			for index, action := range path.Actions {
				steps[index] = CompletedStep{Action: action, Mode: ProactiveAction}
			}
		}
		for _, step := range steps {
			expectedMode, exists := actions[step.Action.Realization]
			if !exists {
				return &CompileError{
					Category:     ErrorMissingRealization,
					Source:       step.Action.Source,
					Expected:     "action realization",
					Actual:       step.Action.Realization,
					MissingChain: []string{step.Action.Realization},
					Detail:       fmt.Sprintf("missing action realization %q", step.Action.Realization),
				}
			}
			if step.Mode != expectedMode {
				return &CompileError{
					Category: ErrorRealizationModeMismatch,
					Source:   step.Action.Source,
					Expected: actionModeName(step.Mode),
					Actual:   actionModeName(expectedMode),
					Detail:   fmt.Sprintf("action realization %q has mode %s, expected %s", step.Action.Realization, actionModeName(expectedMode), actionModeName(step.Mode)),
				}
			}
		}
		for _, resource := range path.Resources {
			if _, exists := resources[resource.Realization]; exists {
				continue
			}
			return &CompileError{
				Category:     ErrorMissingRealization,
				Source:       resource.Source,
				Expected:     "resource realization",
				Actual:       resource.Realization,
				MissingChain: []string{resource.Name, resource.Realization},
				Detail:       fmt.Sprintf("resource %q has missing realization %q", resource.Name, resource.Realization),
			}
		}
		for _, policy := range path.Policies {
			if _, exists := policies[policy.Realization]; exists {
				continue
			}
			return &CompileError{
				Category:     ErrorMissingRealization,
				Source:       policy.Source,
				Expected:     "policy realization",
				Actual:       policy.Realization,
				MissingChain: []string{policy.Name, policy.Realization},
				Detail:       fmt.Sprintf("policy %q has missing realization %q", policy.Name, policy.Realization),
			}
		}
	}
	return nil
}

func validateRealizationCatalog(catalog RealizationCatalog) error {
	actions := make(map[string]ActionMode, len(catalog.Actions))
	for _, action := range catalog.Actions {
		if action.Name == "" || (action.Mode != ProactiveAction && action.Mode != ReactiveAction && action.Mode != ObservationAction) {
			return invalidRealizationCatalog("action", action.Name, "name and execution mode are required")
		}
		if previous, exists := actions[action.Name]; exists {
			detail := "duplicate action realization"
			if previous != action.Mode {
				detail = fmt.Sprintf("contradictory action modes %s and %s", actionModeName(previous), actionModeName(action.Mode))
			}
			return invalidRealizationCatalog("action", action.Name, detail)
		}
		actions[action.Name] = action.Mode
	}
	if err := validateRealizationNames("policy", catalog.Policies); err != nil {
		return err
	}
	return validateRealizationNames("resource", catalog.Resources)
}

func validateRealizationNames(kind string, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return invalidRealizationCatalog(kind, name, "name is empty")
		}
		if _, exists := seen[name]; exists {
			return invalidRealizationCatalog(kind, name, "duplicate realization")
		}
		seen[name] = struct{}{}
	}
	return nil
}

func invalidRealizationCatalog(kind, name, detail string) error {
	return &CompileError{
		Category: ErrorInvalidRealizationCatalog,
		Actual:   name,
		Detail:   fmt.Sprintf("%s %q: %s", kind, name, detail),
	}
}

func actionModeName(mode ActionMode) string {
	switch mode {
	case ProactiveAction:
		return "proactive"
	case ReactiveAction:
		return "reactive"
	case ObservationAction:
		return "observation"
	default:
		return fmt.Sprintf("unknown(%d)", mode)
	}
}
