package umpiretest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/canary"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2"
)

type canaryRegressionEnvironment struct {
	umpire2.RegressionEnvironment
	name namespace.Name
}

func (e *canaryRegressionEnvironment) Namespace() namespace.Name {
	return e.name
}

func TestCanaryDriverValidatesConfiguration(t *testing.T) {
	profile, err := umpire.ForEnvironment(umpire.CanaryEnvironment, umpire.InProcessProfile())
	require.NoError(t, err)
	options := CanaryDriverOptions{ModelVersion: "model/v1", Profile: coreregress.Profile{Environment: profile}, Plan: func(canary.Action) (coreregress.CompletedPath, error) {
		return coreregress.CompletedPath{}, nil
	}}

	_, err = NewCanaryDriver(nil, options)
	require.ErrorContains(t, err, "environment factory is nil")
	factory := func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		return nil, nil, nil
	}
	options.ModelVersion = ""
	_, err = NewCanaryDriver(factory, options)
	require.ErrorContains(t, err, "model version is empty")
}

func TestCanaryDriverRunsInsideCommonSafetyEnvelopeAndCleansUp(t *testing.T) {
	profile, err := umpire.ForEnvironment(umpire.CanaryEnvironment, umpire.InProcessProfile())
	require.NoError(t, err)
	cleaned := false
	factory := func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		return &canaryRegressionEnvironment{name: namespace.Name("isolated")}, func(context.Context) error {
			cleaned = true
			return nil
		}, nil
	}
	driver, err := NewCanaryDriver(factory, CanaryDriverOptions{
		ModelVersion: "model/v1",
		Profile:      coreregress.Profile{Name: "canary", Environment: profile},
		Plan: func(canary.Action) (coreregress.CompletedPath, error) {
			return coreregress.CompletedPath{}, errors.New("operation has no approved completed path")
		},
	})
	require.NoError(t, err)

	result, err := canary.Run(t.Context(), canary.Request{
		Environment: profile,
		Envelope: canary.SafetyEnvelope{
			CampaignID: "campaign", Namespace: "isolated", Tenant: "tenant",
			NamespaceIsolated: true, TenantIsolated: true,
			AllowedActions: []string{"observe"}, MaxActions: 1, MaxConcurrent: 1,
			MaxDuration: time.Second, MaxEvidenceBytes: 4096, CleanupTimeout: time.Second,
		},
		Actions: []canary.Action{{Name: "observe", Namespace: "isolated", Tenant: "tenant"}},
		Driver:  driver,
	})
	require.NoError(t, err)
	require.Equal(t, "action failed", result.StopReason)
	require.True(t, result.Cleanup.Complete)
	require.True(t, cleaned)
}

func TestCanaryDriverAttestsActualNamespace(t *testing.T) {
	profile, err := umpire.ForEnvironment(umpire.CanaryEnvironment, umpire.InProcessProfile())
	require.NoError(t, err)
	cleaned := false
	driver, err := NewCanaryDriver(
		func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
			return &canaryRegressionEnvironment{name: namespace.Name("actual")}, func(context.Context) error {
				cleaned = true
				return nil
			}, nil
		},
		CanaryDriverOptions{
			ModelVersion: "model/v1", Profile: coreregress.Profile{Environment: profile},
			Plan: func(canary.Action) (coreregress.CompletedPath, error) { return coreregress.CompletedPath{}, nil },
		},
	)
	require.NoError(t, err)

	result, err := canary.Run(t.Context(), canary.Request{
		Environment: profile,
		Envelope: canary.SafetyEnvelope{
			CampaignID: "campaign", Namespace: "requested", Tenant: "tenant",
			NamespaceIsolated: true, TenantIsolated: true,
			AllowedActions: []string{"observe"}, MaxActions: 1, MaxConcurrent: 1,
			MaxDuration: time.Second, MaxEvidenceBytes: 4096, CleanupTimeout: time.Second,
		},
		Actions: []canary.Action{{Name: "observe", Namespace: "requested", Tenant: "tenant"}},
		Driver:  driver,
	})
	require.NoError(t, err)
	require.Equal(t, "isolation attestation mismatch", result.StopReason)
	require.True(t, cleaned)
}
