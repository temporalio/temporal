package workflowresend

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/consts"
	"go.uber.org/mock/gomock"
)

func TestResolveCurrentChildExecutionOnSource(t *testing.T) {
	for _, tc := range []struct {
		name, firstRunID, runID    string
		infoFallback, routeChanged bool
		rpcErr                     error
	}{
		{name: "successor", firstRunID: syncTestRunID, runID: "successor"},
		{name: "legacy first-run storage", firstRunID: syncTestRunID, runID: "successor", infoFallback: true},
		{name: "reused workflow ID", firstRunID: "unrelated", runID: "unrelated"},
		{name: "missing current", rpcErr: serviceerror.NewNotFound("missing")},
		{name: "remote unavailable", rpcErr: serviceerror.NewUnavailable("unavailable")},
		{name: "empty run ID", firstRunID: syncTestRunID},
		{name: "active changes during lookup", firstRunID: syncTestRunID, runID: "successor", routeChanged: true},
		{name: "active changes while returning NotFound", rpcErr: serviceerror.NewNotFound("missing"), routeChanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSyncWorkflowStateFixture(t)
			entry := syncTestNamespace(syncTestSourceCluster, syncTestSourceCluster, syncTestCurrentCluster)
			f.expectNamespaceLookup(entry, nil)
			f.shard.EXPECT().GetRemoteAdminClient(syncTestSourceCluster).Return(f.remoteClient, nil)
			state := &persistencespb.WorkflowMutableState{ExecutionState: &persistencespb.WorkflowExecutionState{RunId: tc.runID, FirstExecutionRunId: tc.firstRunID}}
			if tc.infoFallback {
				state.ExecutionState.FirstExecutionRunId = ""
				state.ExecutionInfo = &persistencespb.WorkflowExecutionInfo{FirstExecutionRunId: tc.firstRunID}
			}
			f.remoteClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{Namespace: entry.Name().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: syncTestWorkflowID}, Archetype: chasm.WorkflowArchetype, SkipForceReload: true})).Return(&adminservice.DescribeMutableStateResponse{DatabaseMutableState: state}, tc.rpcErr)
			if tc.routeChanged {
				entry = syncTestNamespace(syncTestAlternativeSourceCluster, syncTestAlternativeSourceCluster, syncTestCurrentCluster)
			}
			f.registry.EXPECT().GetNamespaceByID(syncTestNamespaceID).Return(entry, nil)
			execution, err := ResolveCurrentChildExecutionOnSource(t.Context(), f.shard, syncTestNamespaceID, f.execution)
			switch {
			case tc.routeChanged:
				require.ErrorIs(t, err, consts.ErrWorkflowNotReady)
			case tc.rpcErr != nil:
				require.Same(t, tc.rpcErr, err)
			case tc.firstRunID != syncTestRunID:
				require.ErrorIs(t, err, consts.ErrWorkflowNotReady)
			case tc.runID == "":
				require.ErrorAs(t, err, new(*serviceerror.Internal))
			default:
				require.NoError(t, err)
				require.Equal(t, tc.runID, execution.GetRunId())
				require.Equal(t, syncTestWorkflowID, execution.GetWorkflowId())
			}
		})
	}
}
