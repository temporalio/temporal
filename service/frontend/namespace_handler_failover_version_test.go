package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestUpdateNamespace_PassiveClusterFailoverVersionBump covers the self-hosted to cloud migration
// procedure for a global namespace that has failed over before (FailoverVersion > IFV) and whose
// clusters disagree on FailoverVersionIncrement:
//
//	cluster-a: IFV=2, FVI=10,        namespace FailoverVersion=42 (active)
//	cluster-b: IFV=5, FVI=1_000_000, namespace FailoverVersion=42 (passive)
//
// Running `tctl --ns $ns n u --ac a` against passive cluster-b must bump the FailoverVersion to the
// next version owned by cluster-a under cluster-b's FVI (1_000_002), keep cluster-a active, and
// leave everything else untouched. The bumped version is replicated back to cluster-a.
func TestUpdateNamespace_PassiveClusterFailoverVersionBump(t *testing.T) {
	const (
		clusterA = "cluster-a"
		clusterB = "cluster-b"

		nsName = "migrating-ns"
		nsID   = "a5c4e1d2-0000-4000-8000-000000000001"

		configVersion               = int64(3)
		failoverNotificationVersion = int64(50)
		notificationVersion         = int64(100)
	)

	// Cluster metadata as seen from cluster-b (the passive cluster the update is sent to).
	clusterBMetadata := cluster.NewMetadata(
		true,
		1_000_000,
		clusterB,
		clusterB,
		map[string]cluster.ClusterInformation{
			clusterA: {Enabled: true, InitialFailoverVersion: 2, RPCAddress: "cluster-a:7233"},
			clusterB: {Enabled: true, InitialFailoverVersion: 5, RPCAddress: "cluster-b:7233"},
		},
		nil,
		nil,
		log.NewNoopLogger(),
	)

	lastFailoverTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newNamespaceDetail := func(failoverVersion int64) *persistencespb.NamespaceDetail {
		return &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nsID,
				Name:        nsName,
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "migrating namespace",
				Owner:       "owner@temporal.io",
				Data:        map[string]string{"k": "v"},
			},
			Config: &persistencespb.NamespaceConfig{
				Retention: durationpb.New(72 * time.Hour),
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: clusterA,
				Clusters:          []string{clusterA, clusterB},
				State:             enumspb.REPLICATION_STATE_NORMAL,
				FailoverHistory: []*persistencespb.FailoverStatus{
					{FailoverTime: timestamppb.New(lastFailoverTime), FailoverVersion: failoverVersion},
				},
			},
			ConfigVersion:               configVersion,
			FailoverVersion:             failoverVersion,
			FailoverNotificationVersion: failoverNotificationVersion,
		}
	}

	testCases := []struct {
		name                    string
		currentFailoverVersion  int64
		expectedFailoverVersion int64
		expectFailoverRecorded  bool
	}{
		{
			name:                    "failed over before: bump to next cluster-a version under cluster-b FVI",
			currentFailoverVersion:  42,
			expectedFailoverVersion: 1_000_002,
			expectFailoverRecorded:  true,
		},
		{
			name:                    "already bumped: re-running the update is a no-op for FailoverVersion",
			currentFailoverVersion:  1_000_002,
			expectedFailoverVersion: 1_000_002,
			expectFailoverRecorded:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metadataMgr := persistence.NewMockMetadataManager(ctrl)
			replicationQueue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(now)

			handler := newNamespaceHandler(
				log.NewNoopLogger(),
				wideevents.NoopLogger(),
				metadataMgr,
				namespace.NewMockRegistry(ctrl),
				clusterBMetadata,
				nsreplication.NewReplicator(replicationQueue, log.NewNoopLogger(), nil, dc.GetBoolPropertyFn(false), clusterB),
				archiver.NewArchivalMetadata(dc.NewNoopCollection(), "", false, "", false, &config.ArchivalNamespaceDefaults{}),
				provider.NewMockArchiverProvider(ctrl),
				timeSource,
				NewConfig(dc.NewNoopCollection(), 1024),
				nil,
				metrics.NoopMetricsHandler,
			)

			metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{
				NotificationVersion: notificationVersion,
			}, nil)
			metadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
				Namespace:         newNamespaceDetail(tc.currentFailoverVersion),
				IsGlobalNamespace: true,
			}, nil)

			// Everything except FailoverVersion, FailoverNotificationVersion and FailoverHistory stays the same.
			expected := newNamespaceDetail(tc.currentFailoverVersion)
			expected.FailoverVersion = tc.expectedFailoverVersion
			expected.FailoverNotificationVersion = notificationVersion
			if tc.expectFailoverRecorded {
				expected.ReplicationConfig.FailoverHistory = append(
					expected.ReplicationConfig.FailoverHistory,
					&persistencespb.FailoverStatus{FailoverTime: timestamppb.New(now), FailoverVersion: tc.expectedFailoverVersion},
				)
			}

			var persisted *persistence.UpdateNamespaceRequest
			metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
					persisted = request
					return nil
				})
			var replicated *replicationspb.ReplicationTask
			replicationQueue.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, task proto.Message) error {
					replicated = task.(*replicationspb.ReplicationTask)
					return nil
				})

			// b> tctl --ns $ns n u --ac a
			resp, err := handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
				Namespace: nsName,
				ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
					ActiveClusterName: clusterA,
				},
			})
			require.NoError(t, err)

			require.Equal(t, tc.expectedFailoverVersion, resp.GetFailoverVersion())
			require.Equal(t, clusterA, resp.GetReplicationConfig().GetActiveClusterName())

			require.NotNil(t, persisted)
			require.True(t, persisted.IsGlobalNamespace)
			require.Equal(t, notificationVersion, persisted.NotificationVersion)
			protorequire.ProtoEqual(t, expected, persisted.Namespace)

			// The bumped version must still map to cluster-a, so cluster-a stays active after replication.
			require.Equal(t, clusterA, clusterBMetadata.ClusterNameForFailoverVersion(true, tc.expectedFailoverVersion))

			require.NotNil(t, replicated)
			task := replicated.GetNamespaceTaskAttributes()
			require.Equal(t, tc.expectedFailoverVersion, task.GetFailoverVersion())
			require.Equal(t, configVersion, task.GetConfigVersion())
			require.Equal(t, clusterA, task.GetReplicationConfig().GetActiveClusterName())
		})
	}
}
