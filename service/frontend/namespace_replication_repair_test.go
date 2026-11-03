package frontend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestNamespaceRepairReconcilesConcurrentlyAndPreservesPartialResults(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	metadata := persistence.NewMockMetadataManager(controller)
	clusters := cluster.NewMockMetadata(controller)
	visibility := chasm.NewMockVisibilityManager(controller)
	clusters.EXPECT().GetCurrentClusterName().Return("cluster-a")
	metadata.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "namespace-id"}).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: true,
		Namespace: &persistencespb.NamespaceDetail{
			Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cluster-a", Clusters: []string{"cluster-a", "cluster-b"}},
		},
	}, nil)
	memo, err := payload.Encode(&emptypb.Empty{})
	require.NoError(t, err)
	executions := make([]*chasmspb.VisibilityExecutionInfo, 8)
	releases := make(map[string]chan struct{}, len(executions))
	for i := range executions {
		id := fmt.Sprintf("business-%d", i)
		executions[i] = &chasmspb.VisibilityExecutionInfo{
			BusinessId: id, RunId: fmt.Sprintf("run-%d", i), ChasmMemo: memo,
			StartTime: timestamppb.New(time.Unix(1, 0)), CloseTime: timestamppb.New(time.Time{}),
		}
		releases[id] = make(chan struct{})
	}
	visibility.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{Executions: executions}, nil)
	started := make(chan string, len(executions))
	var active, maxActive atomic.Int32
	client := &testNamespaceReplicationRepairClient{call: func(ctx context.Context, request *namespacereplicationpb.ReconcileNamespaceMutationRequest) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, error) {
		count := active.Add(1)
		defer active.Add(-1)
		for previous := maxActive.Load(); count > previous; previous = maxActive.Load() {
			if maxActive.CompareAndSwap(previous, count) {
				break
			}
		}
		started <- request.GetBusinessId()
		select {
		case <-releases[request.GetBusinessId()]:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if request.GetBusinessId() == "business-2" {
			return nil, serviceerror.NewUnavailable("history unavailable")
		}
		return &namespacereplicationpb.ReconcileNamespaceMutationResponse{ComponentEligible: true, ComponentCompleted: true}, nil
	}}
	handler := &AdminHandler{logger: log.NewNoopLogger(), persistenceMetadataManager: metadata, clusterMetadata: clusters, chasmVisibilityManager: visibility, chasmNsReplClient: client}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type result struct {
		response *adminservice.ReconcileNamespaceMutationComponentsResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, callErr := handler.ReconcileNamespaceMutationComponents(ctx, &adminservice.ReconcileNamespaceMutationComponentsRequest{
			NamespaceId: "namespace-id", RepairId: "repair-id", Reason: "repair", CutoffTime: timestamppb.New(time.Unix(10, 0)),
			VerifiedConfigVersion: wrapperspb.Int64(0), VerifiedFailoverVersion: wrapperspb.Int64(0),
			AuthoritativePeerClusters: []string{"cluster-b"}, VerifiedPeerClusters: []string{"cluster-b"},
		})
		done <- result{response, callErr}
	}()
	first := make([]string, 4)
	for i := range first {
		first[i] = await.Rcv(t, started)
	}
	require.ElementsMatch(t, []string{"business-0", "business-1", "business-2", "business-3"}, first)
	require.Equal(t, int32(4), active.Load())
	second := make([]string, 4)
	for i := range first {
		close(releases[first[len(first)-1-i]])
		second[i] = await.Rcv(t, started)
	}
	for _, id := range second {
		close(releases[id])
	}
	got := await.Rcv(t, done)
	require.NoError(t, got.err)
	require.Equal(t, int32(4), maxActive.Load())
	require.Len(t, got.response.GetComponents(), len(executions))
	for i, component := range got.response.GetComponents() {
		require.Equal(t, executions[i].GetBusinessId(), component.GetBusinessId())
		require.Equal(t, executions[i].GetRunId(), component.GetRunId())
		if i == 2 {
			require.Equal(t, "history unavailable", component.GetError())
		} else {
			require.Empty(t, component.GetError())
			require.True(t, component.GetCompleted())
		}
	}
}

func TestNamespaceRepairRequiresVerifiedVersions(t *testing.T) {
	t.Parallel()
	source := clock.NewEventTimeSource()
	source.Update(time.Unix(100, 0))
	handler := &AdminHandler{timeSource: source}
	err := handler.validateNamespaceMutationRepair(t.Context(), &adminservice.ReconcileNamespaceMutationComponentsRequest{
		NamespaceId: "namespace-id", RepairId: "repair-id", Reason: "repair",
		CutoffTime: timestamppb.New(time.Unix(101, 0)),
	})
	require.ErrorContains(t, err, "verified config and failover versions are required")
}

func TestReconcileNamespaceMutationComponentsReturnsOnePageAndMapsResults(t *testing.T) {
	controller := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(controller)
	clusterMetadata := cluster.NewMockMetadata(controller)
	visibilityManager := chasm.NewMockVisibilityManager(controller)
	repairClient := &testNamespaceReplicationRepairClient{}
	handler := &AdminHandler{
		// This frontend clock trails the snapshot's frontend; versions fence coverage.
		timeSource:                 clock.NewEventTimeSource(),
		logger:                     log.NewTestLogger(),
		persistenceMetadataManager: metadataManager,
		clusterMetadata:            clusterMetadata,
		chasmVisibilityManager:     visibilityManager,
		chasmNsReplClient:          repairClient,
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
					State:             enumspb.REPLICATION_STATE_UNSPECIFIED,
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
		NamespaceId:           "namespace-id",
		RepairId:              "repair-id",
		Reason:                "operator repair",
		CutoffTime:            timestamppb.New(time.Unix(10, 0)),
		VerifiedConfigVersion: wrapperspb.Int64(0), VerifiedFailoverVersion: wrapperspb.Int64(0),
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

func TestNamespaceRepairRejectsChangedSourceVersions(t *testing.T) {
	t.Parallel()
	for _, failoverChanged := range []bool{false, true} {
		t.Run(fmt.Sprintf("failoverChanged=%v", failoverChanged), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := persistence.NewMockMetadataManager(ctrl)
			clusters := cluster.NewMockMetadata(ctrl)
			clusters.EXPECT().GetCurrentClusterName().Return("cluster-a")
			detail := &persistencespb.NamespaceDetail{
				Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cluster-a", Clusters: []string{"cluster-a", "cluster-b"}},
			}
			if failoverChanged {
				detail.FailoverVersion = 1
			} else {
				detail.ConfigVersion = 1
			}
			metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{Namespace: detail, IsGlobalNamespace: true}, nil)
			handler := &AdminHandler{clusterMetadata: clusters, persistenceMetadataManager: metadata}
			err := handler.validateNamespaceMutationRepair(t.Context(), &adminservice.ReconcileNamespaceMutationComponentsRequest{
				NamespaceId: "namespace-id", RepairId: "repair-id", Reason: "repair", CutoffTime: timestamppb.New(time.Unix(100, 0)),
				VerifiedConfigVersion: wrapperspb.Int64(0), VerifiedFailoverVersion: wrapperspb.Int64(0), AuthoritativePeerClusters: []string{"cluster-b"},
			})
			require.ErrorContains(t, err, "versions changed after verification")
		})
	}
}

func TestReconcileNamespaceMutationExecutionReportsPerComponentError(t *testing.T) {
	repairClient := &testNamespaceReplicationRepairClient{err: errors.New("history unavailable")}
	handler := &AdminHandler{chasmNsReplClient: repairClient}
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
	namespacereplicationpb.NamespaceReplicationServiceClient
	requests []*namespacereplicationpb.ReconcileNamespaceMutationRequest
	response *namespacereplicationpb.ReconcileNamespaceMutationResponse
	err      error
	call     func(context.Context, *namespacereplicationpb.ReconcileNamespaceMutationRequest) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, error)
}

func (c *testNamespaceReplicationRepairClient) ReconcileNamespaceMutation(
	ctx context.Context,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, error) {
	if c.call != nil {
		return c.call(ctx, request)
	}
	c.requests = append(c.requests, request)
	return c.response, c.err
}
