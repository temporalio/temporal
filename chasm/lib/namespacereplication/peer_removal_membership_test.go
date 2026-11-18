package namespacereplication

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

type dataMembershipResolver struct {
	namespace.ReplicationResolver
	members []string
}

func (r dataMembershipResolver) ClusterNames(string) []string { return slices.Clone(r.members) }

func TestPeerRemovalUsesResolverMembership(t *testing.T) {
	t.Parallel()
	defaultFactory := namespace.NewDefaultReplicationResolverFactory()
	membershipFactory := func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
		resolver := defaultFactory(detail)
		if members, ok := detail.GetInfo().GetData()["members"]; ok {
			return dataMembershipResolver{ReplicationResolver: resolver, members: strings.Split(members, ",")}
		}
		return resolver
	}
	for _, tc := range []struct {
		name           string
		legacyClusters []string
		members        string
		wantRemoved    bool
	}{
		// cellB is a member the deployment added outside ReplicationConfig.Clusters.
		{name: "member outside legacy list is not removed", legacyClusters: []string{"cellA"}, members: "cellA,cellB"},
		// cellB is still listed in the legacy list but the deployment removed it.
		{name: "legacy-listed non-member is removed", legacyClusters: []string{"cellA", "cellB"}, members: "cellA", wantRemoved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			env.peerHandler.replicationResolverFactory = membershipFactory
			mutation := env.mutationUpdate("cellB")
			mutation.NamespaceDetail.ReplicationConfig.Clusters = tc.legacyClusters
			mutation.NamespaceDetail.Info.Data = map[string]string{"members": tc.members}
			ref := env.start(mutation, func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
			})
			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Cond(func(req *adminservice.ApplyNamespaceMutationRequest) bool {
				return req.GetRemovedPeer() == tc.wantRemoved
			})).Return(&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"}))
		})
	}
}

func TestPeerRemovalUsesRecordedRemovedPeers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		recorded    []string
		members     string
		wantRemoved bool
	}{
		// Membership now says cellB is a member, e.g. after a flag that selects
		// membership flipped, but the source recorded the removal.
		{name: "recorded removal wins over current membership", recorded: []string{"cellB"}, members: "cellA,cellB", wantRemoved: true},
		// Membership now says cellB is not a member, but the source did not remove it.
		{name: "recorded member wins over current membership", recorded: []string{}, members: "cellA", wantRemoved: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			env.peerHandler.replicationResolverFactory = func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
				return dataMembershipResolver{
					ReplicationResolver: namespace.NewDefaultReplicationResolverFactory()(detail),
					members:             strings.Split(tc.members, ","),
				}
			}
			mutation := env.mutationUpdate("cellB")
			mutation.RemovedPeers = &namespacereplicationpb.RemovedPeers{Cells: tc.recorded}
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
