package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/restorenamespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const restoreTestRemoteCluster = "remote-cluster"

func (s *operatorHandlerSuite) restoreTestDetail() *persistencespb.NamespaceDetail {
	return &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: s.currentClusterName,
			Clusters:          []string{s.currentClusterName, restoreTestRemoteCluster},
		},
		ConfigVersion: 6,
	}
}

func remoteNamespace(state enumspb.NamespaceState, configVersion int64) *adminservice.GetNamespaceResponse {
	return &adminservice.GetNamespaceResponse{
		Info:          &namespacepb.NamespaceInfo{State: state},
		ConfigVersion: configVersion,
	}
}

func (s *operatorHandlerSuite) TestValidateRestorePeers() {
	testCases := []struct {
		name      string
		remote    *adminservice.GetNamespaceResponse
		remoteErr error
		wantErrAs any
	}{
		{name: "soft-deleted", remote: remoteNamespace(enumspb.NAMESPACE_STATE_SOFT_DELETED, 5)},
		{name: "registered", remote: remoteNamespace(enumspb.NAMESPACE_STATE_REGISTERED, 5)},
		{name: "deleted", remote: remoteNamespace(enumspb.NAMESPACE_STATE_DELETED, 5), wantErrAs: new(*serviceerror.FailedPrecondition)},
		{name: "missing", remoteErr: serviceerror.NewNamespaceNotFound("ns"), wantErrAs: new(*serviceerror.FailedPrecondition)},
		{name: "unreachable", remoteErr: serviceerror.NewUnavailable("down"), wantErrAs: new(*serviceerror.Unavailable)},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockResource.RemoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
				Attributes: &adminservice.GetNamespaceRequest_Id{Id: "ns-id"},
			}).Return(tc.remote, tc.remoteErr)

			err := s.handler.validateRestorePeers(s.T().Context(), s.restoreTestDetail())
			if tc.wantErrAs == nil {
				s.NoError(err)
				return
			}
			s.ErrorAs(err, tc.wantErrAs)
		})
	}
}

func (s *operatorHandlerSuite) TestAwaitRestoreConvergence() {
	s.Run("converges", func() {
		s.handler.config.NamespaceRestoreConvergenceWait = dynamicconfig.GetDurationPropertyFn(5 * time.Second)
		detail := s.restoreTestDetail()
		s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).
			Return(&persistence.GetNamespaceResponse{Namespace: detail}, nil).Times(2)
		gomock.InOrder(
			s.mockResource.RemoteAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).
				Return(remoteNamespace(enumspb.NAMESPACE_STATE_SOFT_DELETED, 5), nil),
			s.mockResource.RemoteAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).
				Return(remoteNamespace(enumspb.NAMESPACE_STATE_REGISTERED, 6), nil),
		)

		s.NoError(s.handler.awaitRestoreConvergence(s.T().Context(), detail))
	})
	s.Run("lagging cluster", func() {
		s.handler.config.NamespaceRestoreConvergenceWait = dynamicconfig.GetDurationPropertyFn(0)
		detail := s.restoreTestDetail()
		s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).
			Return(&persistence.GetNamespaceResponse{Namespace: detail}, nil)
		s.mockResource.RemoteAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).
			Return(remoteNamespace(enumspb.NAMESPACE_STATE_SOFT_DELETED, 5), nil)

		err := s.handler.awaitRestoreConvergence(s.T().Context(), detail)
		var unavailable *serviceerror.Unavailable
		s.Require().ErrorAs(err, &unavailable)
		s.Contains(unavailable.Message, "remote-cluster: SoftDeleted, config_version 5 < 6")
	})
}

func (s *operatorHandlerSuite) restoredNamespace(restore *persistencespb.NamespaceRestoreInfo) *namespace.Namespace {
	detail := s.restoreTestDetail()
	detail.Info.RestoreInfo = restore
	ns, err := namespace.FromPersistentState(detail, namespace.NewDefaultReplicationResolverFactory()(detail))
	s.NoError(err)
	return ns
}

func (s *operatorHandlerSuite) TestDescribeNamespaceRestore_NeverRestored() {
	s.mockResource.NamespaceCache.EXPECT().GetNamespace(namespace.Name("ns")).Return(s.restoredNamespace(nil), nil)

	_, err := s.handler.DescribeNamespaceRestore(s.T().Context(), &operatorservice.DescribeNamespaceRestoreRequest{Namespace: "ns"})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
}

func (s *operatorHandlerSuite) TestDescribeNamespaceRestore_LocalJob() {
	startTime := timestamppb.New(time.Now().Add(-time.Hour))
	restore := &persistencespb.NamespaceRestoreInfo{
		RestoreId:   "restore-1",
		HostCluster: s.currentClusterName,
		State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
		StartTime:   startTime,
	}
	workflowID := restorenamespace.WorkflowID("ns-id", "restore-1")
	stats := restorenamespace.RefreshStats{TotalEstimate: 100, ProcessedCount: 40, RefreshStartTime: time.Now().Add(-time.Minute)}

	testCases := []struct {
		name          string
		jobStatus     enumspb.WorkflowExecutionStatus
		jobNotFound   bool
		wantState     enumspb.NamespaceRestoreState
		wantRunID     string
		wantProcessed int64
		wantETA       bool
	}{
		{name: "job not started", jobNotFound: true, wantState: enumspb.NAMESPACE_RESTORE_STATE_RUNNING},
		{name: "job running", jobStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wantState: enumspb.NAMESPACE_RESTORE_STATE_RUNNING, wantRunID: "run-id", wantProcessed: 40, wantETA: true},
		{name: "job terminated", jobStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, wantState: enumspb.NAMESPACE_RESTORE_STATE_FAILED, wantRunID: "run-id", wantProcessed: 40},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockResource.NamespaceCache.EXPECT().GetNamespaceByID(namespace.ID("ns-id")).Return(s.restoredNamespace(restore), nil)
			sdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(sdkClient)
			if tc.jobNotFound {
				sdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), workflowID, "").Return(nil, serviceerror.NewNotFound("not found"))
			} else {
				desc := &workflowservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: "run-id"},
						Status:    tc.jobStatus,
					},
				}
				if tc.jobStatus == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
					details, err := payloads.Encode(restorenamespace.RefreshHeartbeat{RefreshStats: stats})
					s.Require().NoError(err)
					desc.PendingActivities = []*workflowpb.PendingActivityInfo{{
						ActivityType:     &commonpb.ActivityType{Name: restorenamespace.RefreshExecutionsActivityName},
						HeartbeatDetails: details,
					}}
				} else {
					memo, err := payload.Encode(stats)
					s.Require().NoError(err)
					desc.WorkflowExecutionInfo.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{restorenamespace.StatsMemoKey: memo}}
				}
				sdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), workflowID, "").Return(desc, nil)
			}

			resp, err := s.handler.DescribeNamespaceRestore(s.T().Context(), &operatorservice.DescribeNamespaceRestoreRequest{NamespaceId: "ns-id"})
			s.Require().NoError(err)
			s.Equal(tc.wantState, resp.GetState())
			s.Equal(s.currentClusterName, resp.GetHostCluster())
			s.Equal(startTime.AsTime(), resp.GetStartTime().AsTime())
			s.Equal(tc.wantRunID, resp.GetRunId())
			s.Equal(tc.wantProcessed, resp.GetProcessedExecutionCount())
			s.Equal(tc.wantETA, resp.GetEstimatedCompletionTime() != nil)
		})
	}
}

func TestEstimateCompletion(t *testing.T) {
	t.Parallel()
	now := time.Now()

	eta, ok := estimateCompletion(restorenamespace.RefreshStats{
		TotalEstimate:    100,
		ProcessedCount:   25,
		RefreshStartTime: now.Add(-10 * time.Minute),
	}, now)
	require.True(t, ok)
	require.Equal(t, now.Add(30*time.Minute), eta)

	_, ok = estimateCompletion(restorenamespace.RefreshStats{TotalEstimate: 100, RefreshStartTime: now.Add(-time.Minute)}, now)
	require.False(t, ok, "no progress yet")

	_, ok = estimateCompletion(restorenamespace.RefreshStats{TotalEstimate: 100, ProcessedCount: 10}, now)
	require.False(t, ok, "refresh not started")
}
