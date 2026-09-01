package mutationtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCampaignVerdictPrecedence(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		stats        mutationRunStats
		wantVerdict  mutationCampaignVerdict
		wantExitCode int
	}{
		{name: "all killed", stats: mutationRunStats{killed: 1}, wantVerdict: campaignClean, wantExitCode: exitMutationKilled},
		{name: "survived", stats: mutationRunStats{survived: 1}, wantVerdict: campaignFindings, wantExitCode: exitMutationSurvived},
		{name: "uncovered", stats: mutationRunStats{uncovered: 1}, wantVerdict: campaignFindings, wantExitCode: exitMutationSurvived},
		{name: "skipped precedes findings", stats: mutationRunStats{skipped: 1, survived: 1, uncovered: 1}, wantVerdict: campaignIncomplete, wantExitCode: exitMutationSkipped},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			verdict := campaignVerdictForStats(testCase.stats)
			require.Equal(t, testCase.wantVerdict, verdict)
			require.Equal(t, testCase.wantExitCode, campaignExitCode(verdict))
		})
	}
}
