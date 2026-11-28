package namespacereplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.uber.org/mock/gomock"
)

func TestPeerRemoval(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		clusters    []string
		recorded    *namespacereplicationpb.RemovedPeers
		wantRemoved bool
	}{
		// The cluster list still names cellB, e.g. because membership comes from
		// elsewhere, but the source recorded the removal.
		{name: "recorded removal wins over the cluster list", clusters: []string{"cellA", "cellB"},
			recorded: &namespacereplicationpb.RemovedPeers{Cells: []string{"cellB"}}, wantRemoved: true},
		{name: "recorded member wins over the cluster list", clusters: []string{"cellA"},
			recorded: &namespacereplicationpb.RemovedPeers{}},
		// Older mutations use the cluster list to infer removal.
		{name: "unrecorded removal follows the cluster list", clusters: []string{"cellA"}, wantRemoved: true},
		{name: "unrecorded member follows the cluster list", clusters: []string{"cellA", "cellB"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			mutation := env.mutationUpdate("cellB")
			mutation.NamespaceDetail.ReplicationConfig.Clusters = tc.clusters
			mutation.RemovedPeers = tc.recorded
			ref := env.start(mutation, func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
			})
			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Cond(func(req *adminservice.ApplyNamespaceMutationRequest) bool {
				return req.GetRemovedPeer() == tc.wantRemoved && req.GetTargetCluster() == "cellB"
			})).Return(&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"}))
		})
	}
}
