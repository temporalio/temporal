package frontend

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

func (s *namespaceHandlerCommonSuite) runningRestoreResponse(nid string, hostCluster string) *persistence.GetNamespaceResponse {
	response := s.registeredNamespaceResponse(nid, "ns")
	response.Namespace.Info.RestoreInfo = &persistencespb.NamespaceRestoreInfo{
		RestoreId:   "restore-1",
		HostCluster: hostCluster,
		State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
	}
	return response
}

// expectNamespaceRead expects the read of an UpdateNamespace write, and stubs the cluster metadata
// its validation reads.
func (s *namespaceHandlerCommonSuite) expectNamespaceRead(response *persistence.GetNamespaceResponse) {
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"cluster1": {Enabled: true, InitialFailoverVersion: 1},
		"cluster2": {Enabled: true, InitialFailoverVersion: 2},
	}).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 7}, nil)
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(response, nil)
}

func (s *namespaceHandlerCommonSuite) expectPublish(verify func(*replicationspb.NamespaceTaskAttributes)) {
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, task *replicationspb.ReplicationTask) error {
			verify(task.GetNamespaceTaskAttributes())
			return nil
		})
}

func (s *namespaceHandlerCommonSuite) TestStartRestore_SoftDeleted() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
	nid := uuid.NewString()
	s.expectNamespaceRead(s.softDeletedNamespaceResponse(nid, "ns"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.State)
			restore := request.Namespace.Info.RestoreInfo
			s.NotEmpty(restore.GetRestoreId())
			s.Equal("cluster1", restore.GetHostCluster())
			s.Equal(enumspb.NAMESPACE_RESTORE_STATE_RUNNING, restore.GetState())
			s.Equal(s.fakeClock.Now().UnixNano(), restore.GetStartTime().AsTime().UnixNano())
			s.Equal(int64(6), request.Namespace.ConfigVersion)
			s.Equal(int64(7), request.NotificationVersion)
			return nil
		})
	s.expectPublish(func(attributes *replicationspb.NamespaceTaskAttributes) {
		s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, attributes.GetInfo().GetState())
		s.Equal(enumspb.NAMESPACE_RESTORE_STATE_RUNNING, attributes.GetRestoreInfo().GetState())
		s.Equal(int64(6), attributes.GetConfigVersion())
	})

	peersValidated := 0
	detail, err := s.handler.startRestore(s.T().Context(), "", namespace.ID(nid), func(*persistencespb.NamespaceDetail) error {
		peersValidated++
		return nil
	})
	s.Require().NoError(err)
	s.Equal(1, peersValidated)
	s.Equal(int64(6), detail.GetConfigVersion())
	s.Equal(enumspb.NAMESPACE_RESTORE_STATE_RUNNING, detail.GetInfo().GetRestoreInfo().GetState())
}

func (s *namespaceHandlerCommonSuite) TestStartRestore_RetryKeepsRunningRestore() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
	s.expectNamespaceRead(s.runningRestoreResponse(uuid.NewString(), "cluster1"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.expectPublish(func(attributes *replicationspb.NamespaceTaskAttributes) {
		s.Equal("restore-1", attributes.GetRestoreInfo().GetRestoreId())
		s.Equal(int64(5), attributes.GetConfigVersion())
	})

	detail, err := s.handler.startRestore(s.T().Context(), "ns", "", func(*persistencespb.NamespaceDetail) error { return nil })
	s.Require().NoError(err)
	s.Equal("restore-1", detail.GetInfo().GetRestoreInfo().GetRestoreId())
	s.Equal(int64(5), detail.GetConfigVersion())
}

func (s *namespaceHandlerCommonSuite) TestStartRestore_SupersedesRestoreHostedElsewhere() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
	s.expectNamespaceRead(s.runningRestoreResponse(uuid.NewString(), "cluster2"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			restore := request.Namespace.Info.RestoreInfo
			s.NotEqual("restore-1", restore.GetRestoreId())
			s.Equal("cluster1", restore.GetHostCluster())
			return nil
		})
	s.expectPublish(func(*replicationspb.NamespaceTaskAttributes) {})

	_, err := s.handler.startRestore(s.T().Context(), "ns", "", func(*persistencespb.NamespaceDetail) error { return nil })
	s.NoError(err)
}

func (s *namespaceHandlerCommonSuite) TestStartRestore_Rejected() {
	peerErr := serviceerror.NewUnavailable("cluster2 unreachable")
	testCases := []struct {
		name           string
		currentCluster string
		mutate         func(*persistence.GetNamespaceResponse)
		peerErr        error
		wantErrAs      any
	}{
		{
			name:      "deprecated",
			mutate:    func(r *persistence.GetNamespaceResponse) { r.Namespace.Info.State = enumspb.NAMESPACE_STATE_DEPRECATED },
			wantErrAs: new(*serviceerror.FailedPrecondition),
		},
		{
			name:      "deleted",
			mutate:    func(r *persistence.GetNamespaceResponse) { r.Namespace.Info.State = enumspb.NAMESPACE_STATE_DELETED },
			wantErrAs: new(*serviceerror.FailedPrecondition),
		},
		{
			name:      "system namespace",
			mutate:    func(r *persistence.GetNamespaceResponse) { r.Namespace.Info.Id = primitives.SystemNamespaceID },
			wantErrAs: new(*serviceerror.FailedPrecondition),
		},
		{
			name:      "peer validation fails",
			peerErr:   peerErr,
			wantErrAs: new(*serviceerror.Unavailable),
		},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
			response := s.softDeletedNamespaceResponse(uuid.NewString(), "ns")
			if tc.mutate != nil {
				tc.mutate(response)
			}
			s.expectNamespaceRead(response)
			s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
			s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

			_, err := s.handler.startRestore(s.T().Context(), "ns", "", func(*persistencespb.NamespaceDetail) error { return tc.peerErr })
			s.ErrorAs(err, tc.wantErrAs)
		})
	}
}

func (s *namespaceHandlerCommonSuite) TestStartRestore_StandbyCluster() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster2").AnyTimes()
	s.expectNamespaceRead(s.softDeletedNamespaceResponse(uuid.NewString(), "ns"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.startRestore(s.T().Context(), "ns", "", func(*persistencespb.NamespaceDetail) error {
		s.Fail("peers validated on a standby cluster")
		return nil
	})
	var notActive *serviceerror.NamespaceNotActive
	s.ErrorAs(err, &notActive)
}

func (s *namespaceHandlerCommonSuite) TestCloseNamespaceRestore() {
	testCases := []struct {
		name         string
		restoreID    string
		currentState enumspb.NamespaceRestoreState
		closeAs      enumspb.NamespaceRestoreState
		wantWrite    bool
		wantPublish  bool
		wantErrAs    any
	}{
		{name: "completes running restore", restoreID: "restore-1", currentState: enumspb.NAMESPACE_RESTORE_STATE_RUNNING, closeAs: enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, wantWrite: true, wantPublish: true},
		{name: "fails running restore", restoreID: "restore-1", currentState: enumspb.NAMESPACE_RESTORE_STATE_RUNNING, closeAs: enumspb.NAMESPACE_RESTORE_STATE_FAILED, wantWrite: true, wantPublish: true},
		{name: "repeated close replicates again", restoreID: "restore-1", currentState: enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, closeAs: enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, wantPublish: true},
		{name: "superseded restore", restoreID: "restore-0", currentState: enumspb.NAMESPACE_RESTORE_STATE_RUNNING, closeAs: enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, wantErrAs: new(*serviceerror.FailedPrecondition)},
		{name: "already failed", restoreID: "restore-1", currentState: enumspb.NAMESPACE_RESTORE_STATE_FAILED, closeAs: enumspb.NAMESPACE_RESTORE_STATE_COMPLETED, wantErrAs: new(*serviceerror.FailedPrecondition)},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			response := s.runningRestoreResponse(uuid.NewString(), "cluster1")
			response.Namespace.Info.RestoreInfo.State = tc.currentState
			s.expectNamespaceRead(response)
			if tc.wantWrite {
				s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
						restore := request.Namespace.Info.RestoreInfo
						s.Equal(tc.closeAs, restore.GetState())
						s.NotNil(restore.GetCloseTime())
						s.Equal(enumspb.NAMESPACE_STATE_REGISTERED, request.Namespace.Info.State)
						s.Equal(int64(6), request.Namespace.ConfigVersion)
						return nil
					})
			} else {
				s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
			}
			if tc.wantPublish {
				s.expectPublish(func(attributes *replicationspb.NamespaceTaskAttributes) {
					s.Equal(tc.closeAs, attributes.GetRestoreInfo().GetState())
				})
			} else {
				s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)
			}

			err := s.handler.CloseNamespaceRestore(s.T().Context(), namespace.ID(response.Namespace.Info.Id), tc.restoreID, tc.closeAs)
			if tc.wantErrAs == nil {
				s.NoError(err)
				return
			}
			s.ErrorAs(err, tc.wantErrAs)
		})
	}
}

func (s *namespaceHandlerCommonSuite) TestCloseNamespaceRestore_InvalidState() {
	err := s.handler.CloseNamespaceRestore(s.T().Context(), "ns-id", "restore-1", enumspb.NAMESPACE_RESTORE_STATE_RUNNING)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *namespaceHandlerCommonSuite) TestSoftDeleteNamespace_FailsRunningRestore() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
	s.expectNamespaceRead(s.runningRestoreResponse(uuid.NewString(), "cluster1"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_SOFT_DELETED, request.Namespace.Info.State)
			restore := request.Namespace.Info.RestoreInfo
			s.Equal(enumspb.NAMESPACE_RESTORE_STATE_FAILED, restore.GetState())
			s.NotNil(restore.GetCloseTime())
			return nil
		})
	s.expectPublish(func(attributes *replicationspb.NamespaceTaskAttributes) {
		s.Equal(enumspb.NAMESPACE_RESTORE_STATE_FAILED, attributes.GetRestoreInfo().GetState())
	})

	s.NoError(s.handler.SoftDeleteNamespace(s.T().Context(), "ns", ""))
}

func (s *namespaceHandlerCommonSuite) TestSoftDeleteNamespace_FailsRestoreOrphanedByFailover() {
	// The restore's host, cluster2, lost the namespace to cluster1 and may be down.
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster1").AnyTimes()
	s.expectNamespaceRead(s.runningRestoreResponse(uuid.NewString(), "cluster2"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(enumspb.NAMESPACE_STATE_SOFT_DELETED, request.Namespace.Info.State)
			s.Equal(enumspb.NAMESPACE_RESTORE_STATE_FAILED, request.Namespace.Info.RestoreInfo.GetState())
			return nil
		})
	s.expectPublish(func(*replicationspb.NamespaceTaskAttributes) {})

	s.NoError(s.handler.SoftDeleteNamespace(s.T().Context(), "ns", ""))
}

func (s *namespaceHandlerCommonSuite) TestSoftDeleteNamespace_RestoreActiveElsewhere() {
	// The namespace is active on cluster1, so the soft delete runs there, whatever the restore's host.
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("cluster2").AnyTimes()
	s.expectNamespaceRead(s.runningRestoreResponse(uuid.NewString(), "cluster2"))
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	err := s.handler.SoftDeleteNamespace(s.T().Context(), "ns", "")
	var activeElsewhere *restoreActiveElsewhereError
	s.Require().ErrorAs(err, &activeElsewhere)
	s.Equal("cluster1", activeElsewhere.activeCluster)
}
