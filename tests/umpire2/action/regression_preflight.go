package action

import (
	"slices"

	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

var regressionRealizations = coreregress.RealizationCatalog{
	Actions: []coreregress.ActionRealization{
		{Name: RegressionNexusScheduleDefault, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusScheduleEmbedded, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusSchedule, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusRespondStartScheduledAsync, Mode: coreregress.ReactiveAction},
		{Name: RegressionNexusRespondStartCompletionPendingAsync, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusRespondStartScheduledSync, Mode: coreregress.ReactiveAction},
		{Name: RegressionNexusCompleteScheduled, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusCompleteStarted, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusCompleteCallbackFailed, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusCancel, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusCancelWithRetry, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusTimeout, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusStartNewHandler, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusStartAttachHandler, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusCompleteFromHandler, Mode: coreregress.ProactiveAction},
		{Name: RegressionNexusStartActivity, Mode: coreregress.ReactiveAction},
		{Name: RegressionWorkflowComplete, Mode: coreregress.ProactiveAction},
		{Name: RegressionWorkflowObserveRunID, Mode: coreregress.ObservationAction},
		{Name: RegressionObserve, Mode: coreregress.ObservationAction},
	},
	Resources: []string{
		RegressionResourceNamespace,
		RegressionResourceTaskQueue,
		RegressionResourceWorker,
		RegressionResourceNexusEndpoint,
		RegressionResourceFaultInjector,
	},
	Policies: []string{
		RegressionPolicyNexusDrop,
		RegressionPolicyNexusFailNext,
	},
}

// RegressionRealizations returns the executable catalog for the local regression environment.
func RegressionRealizations() coreregress.RealizationCatalog {
	result := regressionRealizations
	result.Actions = slices.Clone(result.Actions)
	result.Resources = slices.Clone(result.Resources)
	result.Policies = slices.Clone(result.Policies)
	return result
}

func (h *RegressionHarness) Preflight(suite coreregress.Suite) error {
	h.environmentProfile = suite.Profile.Environment
	h.modelVersion = suite.ModelVersion
	return coreregress.ValidateRealizations(suite, regressionRealizations)
}
