package disabletimeskipping

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestInvoke(t *testing.T) {
	t.Run("invalid namespace", func(t *testing.T) {
		_, err := Invoke(t.Context(), &historyservice.DisableTimeSkippingRequest{}, nil, nil)
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
	})

	for _, tc := range []struct {
		name              string
		disable           bool
		expectPersistence bool
	}{
		{name: "not enabled", disable: false, expectPersistence: false},
		{name: "enabled", disable: true, expectPersistence: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			namespaceRegistry := namespace.NewMockRegistry(controller)
			namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil)
			shardContext := historyi.NewMockShardContext(controller)
			shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry)
			shardContext.EXPECT().GetClusterMetadata().Return(
				clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true)),
			)

			mutableState := historyi.NewMockMutableState(controller)
			mutableState.EXPECT().DisableTimeSkipping().Return(tc.disable)
			workflowContext := historyi.NewMockWorkflowContext(controller)
			if tc.expectPersistence {
				workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), shardContext).Return(nil)
				shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger())
			}
			lease := api.NewWorkflowLease(workflowContext, func(error) {}, mutableState)
			consistencyChecker := api.NewMockWorkflowConsistencyChecker(controller)
			const archetypeID = chasm.ArchetypeID(42)
			consistencyChecker.EXPECT().GetChasmLease(
				gomock.Any(),
				nil,
				gomock.Any(),
				archetypeID,
				locks.PriorityHigh,
			).Return(lease, nil)

			response, err := Invoke(t.Context(), &historyservice.DisableTimeSkippingRequest{
				NamespaceId: tests.GlobalNamespaceEntry.ID().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				ArchetypeId: uint32(archetypeID),
			}, shardContext, consistencyChecker)
			require.NoError(t, err)
			require.Equal(t, tc.disable, response.GetDisabled())
		})
	}
}
