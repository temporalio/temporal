package persistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testcontext"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceReplicationQueuePublishVisibilityTime(t *testing.T) {
	tests := []struct {
		name               string
		visibilityTime     *timestamppb.Timestamp
		wantPreservedValue bool
	}{
		{name: "stamps missing visibility time"},
		{
			name:               "preserves existing visibility time",
			visibilityTime:     timestamppb.New(time.Unix(123, 456)),
			wantPreservedValue: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := mock.NewMockQueue(ctrl)
			serializer := serialization.NewSerializer()
			store.EXPECT().Init(gomock.Any(), gomock.Any()).Return(nil)
			queue, err := persistence.NewNamespaceReplicationQueue(
				store,
				serializer,
				"cluster-a",
				metrics.NoopMetricsHandler,
				log.NewNoopLogger(),
			)
			require.NoError(t, err)

			var encodedTask *commonpb.DataBlob
			store.EXPECT().EnqueueMessage(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, blob *commonpb.DataBlob) error {
					encodedTask = blob
					return nil
				},
			)
			task := &replicationspb.ReplicationTask{
				TaskType:       enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
				VisibilityTime: test.visibilityTime,
			}
			beforePublish := time.Now()

			require.NoError(t, queue.Publish(context.Background(), task))

			publishedTask, err := serializer.ReplicationTaskFromBlob(encodedTask)
			require.NoError(t, err)
			if test.wantPreservedValue {
				require.Equal(t, test.visibilityTime.AsTime(), publishedTask.GetVisibilityTime().AsTime())
			} else {
				require.WithinDuration(t, beforePublish, publishedTask.GetVisibilityTime().AsTime(), time.Second)
			}
		})
	}
}

func TestNamespaceReplicationQueueDeleteAckLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ackLevels  map[string]int64
		cluster    string
		wantLevels map[string]int64
		wantUpdate bool
	}{
		{
			name:       "delete existing ack level",
			ackLevels:  map[string]int64{"cluster-a": 10, "cluster-b": 20},
			cluster:    "cluster-a",
			wantLevels: map[string]int64{"cluster-b": 20},
			wantUpdate: true,
		},
		{
			name:       "ignore missing ack level",
			ackLevels:  map[string]int64{"cluster-b": 20},
			cluster:    "cluster-a",
			wantLevels: map[string]int64{"cluster-b": 20},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			store := mock.NewMockQueue(ctrl)
			serializer := serialization.NewSerializer()
			store.EXPECT().Init(gomock.Any(), gomock.Any()).Return(nil)
			queue, err := persistence.NewNamespaceReplicationQueue(
				store,
				serializer,
				"cluster-a",
				metrics.NoopMetricsHandler,
				log.NewNoopLogger(),
			)
			require.NoError(t, err)

			blob, err := serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{
				ClusterAckLevels: test.ackLevels,
			})
			require.NoError(t, err)
			store.EXPECT().GetAckLevels(gomock.Any()).Return(&persistence.InternalQueueMetadata{
				Blob:    blob,
				Version: 1,
			}, nil)
			if test.wantUpdate {
				store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, metadata *persistence.InternalQueueMetadata) error {
						updated, err := serializer.QueueMetadataFromBlob(metadata.Blob)
						require.NoError(t, err)
						require.Equal(t, test.wantLevels, updated.ClusterAckLevels)
						return nil
					},
				)
			}

			require.NoError(t, queue.DeleteAckLevel(testcontext.For(t), test.cluster))
		})
	}
}

func TestNamespaceReplicationQueueDeleteAckLevelRetriesConflict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := mock.NewMockQueue(ctrl)
	serializer := serialization.NewSerializer()
	store.EXPECT().Init(gomock.Any(), gomock.Any()).Return(nil)
	queue, err := persistence.NewNamespaceReplicationQueue(
		store,
		serializer,
		"cluster-a",
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)

	blob, err := serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{
		ClusterAckLevels: map[string]int64{"cluster-a": 10},
	})
	require.NoError(t, err)
	gomock.InOrder(
		store.EXPECT().GetAckLevels(gomock.Any()).Return(
			&persistence.InternalQueueMetadata{Blob: blob, Version: 1}, nil,
		),
		store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).Return(
			&persistence.ConditionFailedError{Msg: "conflict"},
		),
		store.EXPECT().GetAckLevels(gomock.Any()).Return(
			&persistence.InternalQueueMetadata{Blob: blob, Version: 1}, nil,
		),
		store.EXPECT().UpdateAckLevel(gomock.Any(), gomock.Any()).Return(nil),
	)

	require.NoError(t, queue.DeleteAckLevel(testcontext.For(t), "cluster-a"))
}
