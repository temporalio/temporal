package passivepath

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newSingleClusterWithGlobalNamespace(t *testing.T, logger log.Logger) *testcore.TestCluster {
	clusterName := "passivepath_" + common.GenerateRandomString(5)

	persistenceDefaults := testcore.GetPersistenceTestDefaults()
	persistenceDefaults.DBName += "_" + clusterName

	config := &testcore.TestClusterConfig{
		ClusterMetadata: cluster.Config{
			EnableGlobalNamespace:    true,
			FailoverVersionIncrement: 10,
			MasterClusterName:        clusterName,
			CurrentClusterName:       clusterName,
			ClusterInformation: map[string]cluster.ClusterInformation{
				clusterName: {
					Enabled:                true,
					InitialFailoverVersion: 1,
				},
			},
		},
		HistoryConfig: testcore.HistoryConfig{NumHistoryShards: 1},
		Persistence:   persistenceDefaults,
		DynamicConfigOverrides: map[dynamicconfig.Key]any{
			dynamicconfig.EnableTransitionHistory.Key(): true,
		},
		EnableHistoryTaskRecorder: true,
	}

	tc, err := testcore.NewTestClusterFactory().NewCluster(t, config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tc.TearDownCluster() })
	return tc
}

func registerGlobalNamespace(t *testing.T, tc *testcore.TestCluster, name string) namespace.ID {
	clusterName := tc.ClusterName()
	_, err := tc.FrontendClient().RegisterNamespace(
		testcore.NewContext(),
		&workflowservice.RegisterNamespaceRequest{
			Namespace:                        name,
			IsGlobalNamespace:                true,
			ActiveClusterName:                clusterName,
			Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: clusterName}},
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
	require.NoError(t, err)

	describeResponse, err := tc.FrontendClient().DescribeNamespace(
		testcore.NewContext(),
		&workflowservice.DescribeNamespaceRequest{Namespace: name},
	)
	require.NoError(t, err)
	namespaceID := namespace.ID(describeResponse.GetNamespaceInfo().GetId())

	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	searchAttributes := searchattribute.TestSearchAttributesToRegister()
	searchAttributes["SimulatedFailure"] = enumspb.INDEXED_VALUE_TYPE_BOOL
	_, err = tc.OperatorClient().AddSearchAttributes(
		testcore.NewContext(),
		&operatorservice.AddSearchAttributesRequest{
			Namespace:        name,
			SearchAttributes: searchAttributes,
		},
	)
	require.NoError(t, err)
	return namespaceID
}
