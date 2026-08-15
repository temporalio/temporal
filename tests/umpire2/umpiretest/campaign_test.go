package umpiretest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/campaign"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2"
)

func TestCampaignExecutorValidatesConfiguration(t *testing.T) {
	_, err := NewCampaignExecutor(nil, coreregress.Profile{Environment: umpire.InProcessProfile()}, CampaignExecutorOptions{})
	require.ErrorContains(t, err, "environment factory is nil")

	factory := func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		return nil, nil, nil
	}
	_, err = NewCampaignExecutor(factory, coreregress.Profile{}, CampaignExecutorOptions{})
	require.ErrorContains(t, err, "name is empty")
	_, err = NewCampaignExecutor(factory, coreregress.Profile{Environment: umpire.InProcessProfile()}, CampaignExecutorOptions{
		RunOptions: coreregress.RunOptions{MaxParallel: 2},
	})
	require.ErrorContains(t, err, "must execute serially")
}

func TestCampaignExecutorRejectsUnrealizedScenarioSelections(t *testing.T) {
	factory := func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		t.Fatal("unsupported scenario must be rejected before environment allocation")
		return nil, nil, nil
	}
	executor, err := NewCampaignExecutor(factory, coreregress.Profile{Environment: umpire.InProcessProfile()}, CampaignExecutorOptions{})
	require.NoError(t, err)

	execution := executor.Execute(t.Context(), campaign.Scenario{
		Name: "faulted", ModelVersion: "model/v1", Faults: []string{"drop"},
	})
	require.Equal(t, umpire.ClaimUnsupported, execution.Claim.Status)
	require.Contains(t, execution.Claim.Diagnostic, "explicit campaign scenario transformer")
	require.Empty(t, execution.Error)
}

func TestCampaignExecutorUsesExplicitScenarioTransformer(t *testing.T) {
	want := coreregress.CompletedPath{Actions: []coreregress.CompletedAction{{Name: "transformed"}}}
	transformed, err := exactCampaignPath(campaign.Scenario{Path: want})
	require.NoError(t, err)
	require.Equal(t, want, transformed)

	_, err = exactCampaignPath(campaign.Scenario{ExplorationRoute: []string{"advance"}})
	require.ErrorContains(t, err, "exploration routes")
}
