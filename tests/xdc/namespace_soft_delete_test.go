package xdc

import (
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

func (s *FunctionalClustersTestSuite) TestSoftDeleteNamespace_ReplicatesAndSurvivesFailover() {
	namespace := s.createGlobalNamespace()

	_, err := s.clusters[0].OperatorClient().SoftDeleteNamespace(testcore.NewContext(), &operatorservice.SoftDeleteNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_SOFT_DELETED)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)
	s.requireNamespaceStateOnAllClusters(namespace, enumspb.NAMESPACE_STATE_SOFT_DELETED)
}

func (s *FunctionalClustersTestSuite) requireNamespaceStateOnAllClusters(namespace string, state enumspb.NamespaceState) {
	await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
		for _, c := range s.clusters {
			resp := s.describeNamespace(t, c, namespace, true)
			require.Equal(t, state, resp.GetNamespaceInfo().GetState(), "cluster %s", c.ClusterName())
		}
	}, replicationWaitTime, replicationCheckInterval)
}
