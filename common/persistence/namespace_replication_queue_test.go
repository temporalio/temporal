package persistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
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
