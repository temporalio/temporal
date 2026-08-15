package umpiretest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2"
)

func TestRunRegressionRejectsInvalidRequestBeforeAllocation(t *testing.T) {
	protocol, err := umpire2.DefaultProtocol()
	require.NoError(t, err)
	allocated := false
	_, err = RunRegression(t.Context(), RegressionRequest{
		Protocol: protocol,
		Plan:     coreregress.OnePath(),
		Profile:  localRegressionProfile(true),
		Environment: func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
			allocated = true
			return nil, nil, nil
		},
		RunOptions: coreregress.RunOptions{},
	})
	require.ErrorContains(t, err, "MaxParallel must be positive")
	require.False(t, allocated)
}

func TestLocalRegressionProfileDerivesCapabilitiesFromPreset(t *testing.T) {
	require.ElementsMatch(t, []string{"CHASM", "ActivityCallbacks", "Faults"}, localRegressionProfile(true).Capabilities)
	require.Equal(t, []string{"Faults"}, localRegressionProfile(false).Capabilities)
}
