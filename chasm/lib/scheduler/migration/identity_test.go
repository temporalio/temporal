package migration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSameTimePendingStartsReceiveUniqueIdentities(t *testing.T) {
	when := timestamppb.New(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	starts := []*schedulespb.BufferedStart{
		{NominalTime: when, ActualTime: when},
		{NominalTime: when, ActualTime: when},
	}

	converted := convertBufferedStartsLegacyToCHASM(
		starts,
		"namespace-id",
		"schedule-id",
		1,
		"workflow-id",
	)
	require.Len(t, converted, 2)
	require.NotEqual(t, converted[0].GetRequestId(), converted[1].GetRequestId(),
		"distinct migrated actions must have distinct completion identities")
	require.NotEqual(t, converted[0].GetWorkflowId(), converted[1].GetWorkflowId(),
		"distinct migrated actions must not collide at workflow start")
}
