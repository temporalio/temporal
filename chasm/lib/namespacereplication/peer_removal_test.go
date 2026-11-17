package namespacereplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.uber.org/mock/gomock"
)

func TestPeerDeliveryConveysRemovalIntent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		removed bool
		shadow  bool
		create  bool
	}{
		{name: "removed peer", removed: true},
		{name: "retained peer"},
		{name: "shadow removal", removed: true, shadow: true},
		{name: "create does not carry removal", removed: true, create: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			mutation := env.mutationUpdate("cellB")
			mutation.Shadow = tc.shadow
			if tc.create {
				mutation.Operation = enumsspb.NAMESPACE_OPERATION_CREATE
			}
			if tc.removed {
				mutation.NamespaceDetail.ReplicationConfig.Clusters = []string{"cellA"}
			}
			ref := env.start(mutation, func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
				if tc.shadow {
					c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW
				}
			})
			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			outcome := adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED
			if tc.shadow {
				outcome = adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH
			}
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Cond(func(req *adminservice.ApplyNamespaceMutationRequest) bool {
				return req.GetRemovedPeer() == (tc.removed && !tc.shadow && !tc.create)
			})).Return(&adminservice.ApplyNamespaceMutationResponse{Outcome: outcome}, nil)
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"}))
		})
	}
}
