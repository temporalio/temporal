package xdc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

type (
	// RemoveRemoteClusterTestSuite exercises the namespace-reference guard that
	// RemoveRemoteCluster runs before disconnecting a remote cluster (see
	// service/frontend/cluster_removal_validation.go). It is an end-to-end
	// regression suite for temporalio/temporal#10997, which fixed cluster removal
	// over-blocking on stale/orphaned namespace records.
	//
	// It uses three clusters because the interesting case -- an orphaned namespace
	// that references the cluster being removed but that the current cluster is no
	// longer a member of -- can only be represented by a multi-cluster namespace
	// listing two clusters, neither of which is the cluster running the removal.
	RemoveRemoteClusterTestSuite struct {
		xdcBaseSuite
	}
)

func TestRemoveRemoteClusterTestSuite(t *testing.T) {
	t.Parallel()
	s := new(RemoveRemoteClusterTestSuite)
	suite.Run(t, s)
}

func (s *RemoveRemoteClusterTestSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	// Three clusters are required to reproduce the orphaned-namespace case: the
	// orphan record must be a multi-cluster global namespace that lists two
	// clusters, neither of which is the cluster running RemoveRemoteCluster.
	s.clusterNames = []string{"cluster0", "cluster1", "cluster2"}
	s.setupSuite()
}

func (s *RemoveRemoteClusterTestSuite) SetupTest() {
	s.setupTest()
}

func (s *RemoveRemoteClusterTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestRemoveRemoteCluster_BlockedByReferencingNamespace guards the still-correct
// half of the validation: when a global namespace lists both the current cluster
// and the cluster being removed, the current cluster is actively relying on that
// replication link, so the removal must stay blocked.
func (s *RemoveRemoteClusterTestSuite) TestRemoveRemoteCluster_BlockedByReferencingNamespace() {
	ctx := testcore.NewContext()

	cluster0 := s.clusters[0] // where RemoveRemoteCluster is invoked
	cluster1 := s.clusters[1] // the cluster to be removed

	// Global namespace active on cluster0 and replicated to cluster1. cluster0 is
	// a member and depends on cluster1, so removing cluster1 must be rejected.
	ns := s.createNamespace(true, []*testcore.TestCluster{cluster0, cluster1})

	_, err := cluster0.AdminClient().RemoveRemoteCluster(ctx, &adminservice.RemoveRemoteClusterRequest{
		ClusterName: cluster1.ClusterName(),
	})
	s.Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.Contains(err.Error(), "cannot remove cluster")
	// The rejection must name the namespace that still binds the two clusters.
	s.Contains(err.Error(), ns)
}

// TestRemoveRemoteCluster_OrphanedNamespaceDoesNotBlock is the direct regression
// test for #10997. It reproduces the orphaned namespace record that caused the
// over-blocking incident and asserts that removal now succeeds.
//
// Before #10997 this call returned a FailedPrecondition because the local
// registry still listed the cluster being removed, even though the current
// cluster no longer participated in that namespace.
func (s *RemoveRemoteClusterTestSuite) TestRemoveRemoteCluster_OrphanedNamespaceDoesNotBlock() {
	ctx := testcore.NewContext()

	cluster0 := s.clusters[0] // where RemoveRemoteCluster is invoked
	cluster2 := s.clusters[2] // the cluster to be removed

	// Create a global namespace spanning all three clusters, active on cluster0.
	ns := s.createNamespace(true, s.clusters)

	// Re-home the namespace off cluster0, leaving behind the orphaned record that
	// motivated the fix. A namespace's active cluster must remain in its cluster
	// list, so cluster0 cannot be dropped while active: first fail the namespace
	// over to cluster1, then shrink the cluster list to {cluster1, cluster2}. The
	// shrink is replicated to cluster0 and applied to its local namespace record,
	// but the record itself is never deleted -- so cluster0's registry now holds a
	// multi-cluster namespace that references cluster2 while cluster0 itself is no
	// longer a member. This mirrors an orphan left behind after a namespace
	// migration re-homes a namespace onto other clusters.
	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)
	s.updateNamespaceClusters(ns, 0, []*testcore.TestCluster{s.clusters[1], s.clusters[2]})

	// Wait until cluster0 observes the orphaned record (itself dropped from the
	// namespace's cluster list). updateNamespaceClusters only waits for the
	// clusters that remain in the list, so wait for cluster0 explicitly, then let
	// its namespace cache refresh so RemoveRemoteCluster reads the updated view.
	await.Require(context.Background(), s.T(), func(t *await.T) {
		resp := s.describeNamespace(t, cluster0, ns, true)
		require.ElementsMatch(t,
			[]string{s.clusters[1].ClusterName(), s.clusters[2].ClusterName()},
			s.namespaceClusterNames(resp))
	}, replicationWaitTime, replicationCheckInterval)
	s.waitForNamespaceCacheRefresh()

	// cluster0 is not a member of the orphaned namespace, so removing cluster2
	// cannot sever any replication cluster0 relies on -> removal must succeed.
	_, err := cluster0.AdminClient().RemoveRemoteCluster(ctx, &adminservice.RemoveRemoteClusterRequest{
		ClusterName: cluster2.ClusterName(),
	})
	s.NoError(err)
}
