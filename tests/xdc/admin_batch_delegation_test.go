package xdc

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type AdminBatchDelegationTestSuite struct {
	xdcBaseSuite
}

func TestAdminBatchDelegationTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &AdminBatchDelegationTestSuite{})
}

func (s *AdminBatchDelegationTestSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	s.dynamicConfigOverrides[dynamicconfig.FrontendMaxConcurrentAdminBatchOperation.Key()] = 10
	s.setupSuite()
}

func (s *AdminBatchDelegationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *AdminBatchDelegationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestDelegatedBatchOperation_OnlyInActiveCluster covers the constraint that separates a
// delegated operation from refresh tasks: terminate mutates workflow state, so only the cluster
// that is active for the target namespace may run it. Refresh tasks, by contrast, is reachable
// from the passive cluster, which is why admin batches exist at all.
func (s *AdminBatchDelegationTestSuite) TestDelegatedBatchOperation_OnlyInActiveCluster() {
	ctx := testcore.NewContext()
	ns := s.createGlobalNamespace()

	// The premise: cluster 0 is active for ns, cluster 1 is passive.
	for _, cluster := range s.clusters {
		resp, err := cluster.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
		s.NoError(err)
		s.Equal(s.clusters[0].ClusterName(), resp.GetReplicationConfig().GetActiveClusterName(),
			"%s should see %s as active for %s", cluster.ClusterName(), s.clusters[0].ClusterName(), ns)
	}

	visibilityQuery := fmt.Sprintf("WorkflowType = 'admin-batch-user-op-%s'", uuid.NewString())

	terminateRequest := func() *adminservice.StartAdminBatchOperationRequest {
		return &adminservice.StartAdminBatchOperationRequest{
			Namespace:       ns,
			VisibilityQuery: visibilityQuery,
			JobId:           "user-batch-" + uuid.NewString(),
			Reason:          "xdc admin delegated batch",
			Identity:        "test",
			Operation: &adminservice.StartAdminBatchOperationRequest_DelegationOperation{
				DelegationOperation: &adminservice.BatchOperationDelegation{
					BatchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
				},
			},
		}
	}

	_, err := s.clusters[1].AdminClient().StartAdminBatchOperation(ctx, terminateRequest())
	var notActive *serviceerror.NamespaceNotActive
	s.ErrorAs(err, &notActive,
		"the passive cluster %s must reject a delegated batch operation on %s", s.clusters[1].ClusterName(), ns)

	_, err = s.clusters[0].AdminClient().StartAdminBatchOperation(ctx, terminateRequest())
	s.NoError(err,
		"the active cluster %s must accept a delegated batch operation on %s", s.clusters[0].ClusterName(), ns)

	// Refresh tasks stays reachable from the passive cluster.
	_, err = s.clusters[1].AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:       ns,
		VisibilityQuery: visibilityQuery,
		JobId:           "refresh-" + uuid.NewString(),
		Reason:          "xdc admin batch refresh tasks",
		Identity:        "test",
		Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
			RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
		},
	})
	s.NoError(err,
		"refresh tasks must stay reachable from the passive cluster %s", s.clusters[1].ClusterName())
}
