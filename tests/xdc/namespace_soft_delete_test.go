package xdc

import (
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

func (s *FunctionalClustersTestSuite) TestSoftDeleteNamespace_ReplicatesAndSurvivesFailover() {
	namespace := s.createGlobalNamespace()
	s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.EnableNamespaceSoftDelete, true)

	_, err := s.clusters[0].OperatorClient().SoftDeleteNamespace(testcore.NewContext(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_SOFT_DELETED)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_SOFT_DELETED)
}

func (s *FunctionalClustersTestSuite) TestRestoreNamespace_ReplicatesAndCompletes() {
	namespace := s.createGlobalNamespace()
	s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.EnableNamespaceSoftDelete, true)

	_, err := s.clusters[0].OperatorClient().SoftDeleteNamespace(testcore.NewContext(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_SOFT_DELETED)

	// Only the active cluster can restore.
	_, err = s.clusters[1].OperatorClient().RestoreNamespace(testcore.NewContext(), &operatorservice.RestoreNamespaceRequest{
		Namespace: namespace,
	})
	var notActive *serviceerror.NamespaceNotActive
	s.ErrorAs(err, &notActive)

	await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
		_, err := s.clusters[0].OperatorClient().RestoreNamespace(testcore.NewContext(), &operatorservice.RestoreNamespaceRequest{
			Namespace: namespace,
		})
		require.NoError(t, err)
	}, replicationWaitTime, replicationCheckInterval)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_REGISTERED)

	// The standby forwards to the restore's host for progress.
	await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
		for _, c := range s.clusters {
			resp, err := c.OperatorClient().DescribeNamespaceRestore(testcore.NewContext(), &operatorservice.DescribeNamespaceRestoreRequest{
				Namespace: namespace,
			})
			require.NoError(t, err)
			require.Equal(t, enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, resp.GetState(), "cluster %s", c.ClusterName())
			require.Equal(t, s.clusters[0].ClusterName(), resp.GetHostCluster())
			require.NotEmpty(t, resp.GetRunId(), "cluster %s", c.ClusterName())
		}
	}, replicationWaitTime, replicationCheckInterval)
}

func (s *FunctionalClustersTestSuite) requireNamespaceStateOnAllClusters(namespace string, state enumspb.NamespaceState) {
	await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
		for _, c := range s.clusters {
			resp := s.describeNamespace(t, c, namespace, true)
			require.Equal(t, state, resp.GetNamespaceInfo().GetState(), "cluster %s", c.ClusterName())
		}
	}, replicationWaitTime, replicationCheckInterval)
}
