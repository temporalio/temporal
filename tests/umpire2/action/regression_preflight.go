package action

import (
	"fmt"

	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

type regressionRealizationCatalog struct {
	actions   map[string]coreregress.ActionMode
	resources map[string]struct{}
	policies  map[string]struct{}
}

var regressionRealizations = regressionRealizationCatalog{
	actions: map[string]coreregress.ActionMode{
		RegressionNexusScheduleDefault:                    coreregress.ProactiveAction,
		RegressionNexusScheduleEmbedded:                   coreregress.ProactiveAction,
		RegressionNexusSchedule:                           coreregress.ProactiveAction,
		RegressionNexusRespondStartScheduledAsync:         coreregress.ReactiveAction,
		RegressionNexusRespondStartCompletionPendingAsync: coreregress.ProactiveAction,
		RegressionNexusRespondStartScheduledSync:          coreregress.ReactiveAction,
		RegressionNexusCompleteScheduled:                  coreregress.ProactiveAction,
		RegressionNexusCompleteStarted:                    coreregress.ProactiveAction,
		RegressionNexusCompleteCallbackFailed:             coreregress.ProactiveAction,
		RegressionNexusCancel:                             coreregress.ProactiveAction,
		RegressionNexusCancelWithRetry:                    coreregress.ProactiveAction,
		RegressionNexusTimeout:                            coreregress.ProactiveAction,
		RegressionNexusStartNewHandler:                    coreregress.ProactiveAction,
		RegressionNexusStartAttachHandler:                 coreregress.ProactiveAction,
		RegressionNexusCompleteFromHandler:                coreregress.ProactiveAction,
		RegressionNexusStartActivity:                      coreregress.ReactiveAction,
		RegressionWorkflowComplete:                        coreregress.ProactiveAction,
		RegressionWorkflowObserveRunID:                    coreregress.ObservationAction,
		RegressionObserve:                                 coreregress.ObservationAction,
	},
	resources: map[string]struct{}{
		RegressionResourceNamespace:     {},
		RegressionResourceTaskQueue:     {},
		RegressionResourceWorker:        {},
		RegressionResourceNexusEndpoint: {},
		RegressionResourceFaultInjector: {},
	},
	policies: map[string]struct{}{
		RegressionPolicyNexusDrop:     {},
		RegressionPolicyNexusFailNext: {},
	},
}

func (h *RegressionHarness) Preflight(suite coreregress.Suite) error {
	return regressionRealizations.validate(suite)
}

func (c regressionRealizationCatalog) validate(suite coreregress.Suite) error {
	for pathIndex, path := range suite.Paths {
		steps := path.Steps
		if len(steps) == 0 {
			steps = make([]coreregress.CompletedStep, len(path.Actions))
			for index, completedAction := range path.Actions {
				steps[index] = coreregress.CompletedStep{Action: completedAction, Mode: coreregress.ProactiveAction}
			}
		}
		for stepIndex, step := range steps {
			expectedMode, exists := c.actions[step.Action.Realization]
			if !exists {
				return fmt.Errorf("path %d action %d has unsupported realization %q", pathIndex, stepIndex, step.Action.Realization)
			}
			if step.Mode != expectedMode {
				return fmt.Errorf("path %d action mode for realization %q is %s, expected %s", pathIndex, step.Action.Realization, regressionActionModeName(step.Mode), regressionActionModeName(expectedMode))
			}
		}
		for policyIndex, policy := range path.Policies {
			if _, exists := c.policies[policy.Realization]; !exists {
				return fmt.Errorf("path %d policy %d has unsupported realization %q", pathIndex, policyIndex, policy.Realization)
			}
		}
		for resourceIndex, resource := range path.Resources {
			if _, exists := c.resources[resource.Realization]; !exists {
				return fmt.Errorf("path %d resource %d has unsupported realization %q", pathIndex, resourceIndex, resource.Realization)
			}
		}
	}
	return nil
}

func regressionActionModeName(mode coreregress.ActionMode) string {
	switch mode {
	case coreregress.ProactiveAction:
		return "proactive"
	case coreregress.ReactiveAction:
		return "reactive"
	case coreregress.ObservationAction:
		return "observation"
	default:
		return fmt.Sprintf("unknown(%d)", mode)
	}
}
