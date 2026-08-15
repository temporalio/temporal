package rule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

func TestNexusOperationTimeoutSemanticsValidatesConfiguredStartToCloseTimeout(t *testing.T) {
	tests := []struct {
		name       string
		timeout    enumspb.TimeoutType
		message    string
		violations int
	}{
		{name: "matching metadata", timeout: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, message: "operation timed out"},
		{name: "wrong timeout kind", timeout: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, message: "operation timed out", violations: 1},
		{name: "wrong failure metadata", timeout: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, message: "different failure", violations: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newTestModelState()
			routeFact(t, state, fact.NewNexusOperationHistorySnapshot(
				"namespace-id",
				"workflow-id",
				"5",
				2*time.Second,
				test.timeout,
				test.message,
			))

			violations := checkSafetyRule(state, &NexusOperationTimeoutSemantics{})
			require.Len(t, violations, test.violations)
		})
	}
}
