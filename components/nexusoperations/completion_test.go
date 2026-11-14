package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestCompletionHandlerRejectsInvalidRequestID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, requestID string }{
		{"empty", ""},
		{"mismatched", "other-request-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				RequestId: "request-id", ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))
			err := nexusoperations.CompletionHandler(context.Background(), fakeEnv{node: node}, hsm.Ref{}, tc.requestID, "token", nil, nil, &commonpb.Payload{}, nil)
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			require.Equal(t, enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, op.NexusOperationInfo.State)
		})
	}
}
