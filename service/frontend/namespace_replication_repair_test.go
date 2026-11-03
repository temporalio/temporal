package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReconcileNamespaceMutationComponentsReturnsOnePageAndMapsResults(t *testing.T) {
	controller := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(controller)
	clusterMetadata := cluster.NewMockMetadata(controller)
	visibilityManager := chasm.NewMockVisibilityManager(controller)
	repairClient := &testNamespaceReplicationRepairClient{}
	handler := &AdminHandler{
		persistenceMetadataManager:            metadataManager,
		clusterMetadata:                       clusterMetadata,
		chasmVisibilityManager:                visibilityManager,
		chasmNamespaceReplicationRepairClient: repairClient,
	}

	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster-a").Times(2)
	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "namespace-id"}).Return(
		&persistence.GetNamespaceResponse{
			IsGlobalNamespace: true,
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: "cluster-a",
					Clusters:          []string{"cluster-a", "cluster-b"},
					State:             enumspb.REPLICATION_STATE_NORMAL,
				},
			},
		},
		nil,
	).Times(2)
	memo, err := payload.Encode(&emptypb.Empty{})
	require.NoError(t, err)
	execution := func(businessID, runID string) *chasmspb.VisibilityExecutionInfo {
		return &chasmspb.VisibilityExecutionInfo{
			BusinessId: businessID,
			RunId:      runID,
			StartTime:  timestamppb.New(time.Unix(1, 0)),
			CloseTime:  timestamppb.New(time.Time{}),
			ChasmMemo:  memo,
		}
	}
	visibilityManager.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ any, request *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Empty(t, request.NextPageToken)
			require.Equal(t, 25, request.PageSize)
			require.Contains(t, request.Query, `ReplicatedNamespaceId = "namespace-id"`)
			return &visibilityservice.ListChasmExecutionsResponse{
				Executions:    []*chasmspb.VisibilityExecutionInfo{execution("business-1", "run-1")},
				NextPageToken: []byte("page-2"),
			}, nil
		},
	)
	visibilityManager.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ any, request *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, []byte("page-2"), request.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{
				Executions: []*chasmspb.VisibilityExecutionInfo{execution("business-2", "run-2")},
			}, nil
		},
	)
	repairClient.response = &namespacereplicationpb.ReconcileNamespaceMutationResponse{
		ComponentEligible:  true,
		ComponentCompleted: true,
		Peers: []*namespacereplicationpb.PeerReconciliation{
			{
				TargetCell: "cluster-b",
				Outcome:    namespacereplicationpb.PeerReconciliation_OUTCOME_RECONCILED,
				Basis:      namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET,
			},
		},
	}

	request := &adminservice.ReconcileNamespaceMutationComponentsRequest{
		NamespaceId:               "namespace-id",
		RepairId:                  "repair-id",
		Reason:                    "operator repair",
		CutoffTime:                timestamppb.New(time.Unix(10, 0)),
		AuthoritativePeerClusters: []string{"cluster-b"},
		VerifiedPeerClusters:      []string{"cluster-b"},
		PageSize:                  25,
	}
	response, err := handler.ReconcileNamespaceMutationComponents(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.GetComponents(), 1)
	require.Equal(t, []byte("page-2"), response.GetNextPageToken())
	request.NextPageToken = response.GetNextPageToken()
	response, err = handler.ReconcileNamespaceMutationComponents(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.GetComponents(), 1)
	require.Empty(t, response.GetNextPageToken())
	require.Len(t, repairClient.requests, 2)
	require.Equal(t, "business-1", repairClient.requests[0].GetBusinessId())
	require.Equal(t, "run-2", repairClient.requests[1].GetRunId())
	require.Equal(t, "namespace-id", repairClient.requests[0].GetNamespaceId())
	require.Equal(t, adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED, response.GetComponents()[0].GetPeers()[0].GetOutcome())
	require.Equal(t, adminservice.NamespaceMutationPeerReconciliation_BASIS_VERIFIED_TARGET, response.GetComponents()[0].GetPeers()[0].GetBasis())
}

func TestReconcileNamespaceMutationExecutionReportsPerComponentError(t *testing.T) {
	repairClient := &testNamespaceReplicationRepairClient{err: errors.New("history unavailable")}
	handler := &AdminHandler{chasmNamespaceReplicationRepairClient: repairClient}
	result := handler.reconcileNamespaceMutationExecution(
		context.Background(),
		&adminservice.ReconcileNamespaceMutationComponentsRequest{
			NamespaceId: "namespace-id",
			RepairId:    "repair-id",
			Reason:      "operator repair",
			CutoffTime:  timestamppb.New(time.Unix(10, 0)),
		},
		"business-id",
		"run-id",
	)
	require.Equal(t, "business-id", result.GetBusinessId())
	require.Equal(t, "run-id", result.GetRunId())
	require.Equal(t, "history unavailable", result.GetError())
}

type testNamespaceReplicationRepairClient struct {
	namespacereplicationpb.NamespaceReplicationRepairServiceClient
	requests []*namespacereplicationpb.ReconcileNamespaceMutationRequest
	response *namespacereplicationpb.ReconcileNamespaceMutationResponse
	err      error
}

func (c *testNamespaceReplicationRepairClient) ReconcileNamespaceMutation(
	_ context.Context,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, error) {
	c.requests = append(c.requests, request)
	return c.response, c.err
}
