package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestLaneReceiverFeedbackStreamIdentity(t *testing.T) {
	t.Parallel()
	target := ClusterShardKey{ClusterID: 2, ShardID: 7}
	for _, tc := range []struct {
		name   string
		source ClusterShardKey
	}{
		{name: "first source", source: ClusterShardKey{ClusterID: 1, ShardID: 7}},
		{name: "another cluster", source: ClusterShardKey{ClusterID: 3, ShardID: 7}},
		{name: "unequal shard counts", source: ClusterShardKey{ClusterID: 1, ShardID: 519}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := cluster.NewMockMetadata(ctrl)
			metadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
				"source": {Enabled: true, InitialFailoverVersion: int64(tc.source.ClusterID), ShardCount: 2048},
			})
			scope := LaneFeedbackStream{Source: tc.source, Target: target}
			feedback := NewMockLaneFeedbackProvider(ctrl)
			feedback.EXPECT().RecordTask(scope, "namespace")
			feedback.EXPECT().ThrottledNamespaceIDs(scope).Return([]string{"namespace"})
			feedback.EXPECT().Feedback(scope, 4096).Return(nil)
			config := tests.NewDynamicConfig()
			box := ProcessToolBox{
				Config: config, ClusterMetadata: metadata, Logger: log.NewNoopLogger(),
				MetricsHandler: metrics.NoopMetricsHandler, HighPriorityTaskScheduler: &mockScheduler{},
			}
			flow := NewMockReceiverFlowController(ctrl)
			flow.EXPECT().GetFlowControlInfo(gomock.Any()).Return(FlowControlInfo{
				Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
			}).Times(2)
			r := &laneStreamReceiver{
				ProcessToolBox: box, clientShardKey: target, serverShardKey: tc.source,
				laneFeedback: feedback, taskConverter: NewExecutableTaskConverter(box),
				highPriorityTaskTracker: NewExecutableTaskTracker(box.Logger, box.MetricsHandler),
				lowPriorityTaskTracker:  NewExecutableTaskTracker(box.Logger, box.MetricsHandler),
				flowController:          flow, receiverMode: ReceiverModeTieredStack, lanesEnabled: true, maxLanes: 4,
				logger: box.Logger,
			}
			r.lowPriorityTaskTracker.TrackTasks(WatermarkInfo{Watermark: 10, Timestamp: time.Now()})
			incoming := &mockStream{respChan: make(chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], 1)}
			incoming.respChan <- StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
				Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
					Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
						Messages: &replicationspb.WorkflowReplicationMessages{
							Priority: enumsspb.TASK_PRIORITY_HIGH, ExclusiveHighWatermark: 11,
							ReplicationTasks: []*replicationspb.ReplicationTask{{
								SourceTaskId: 10, Priority: enumsspb.TASK_PRIORITY_HIGH,
								TaskType:    enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
								RawTaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: "namespace"},
								Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
									HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{NamespaceId: "namespace"},
								},
							}},
						},
					},
				},
			}
			close(incoming.respChan)
			require.NoError(t, r.processMessages(incoming))
			outgoing := &mockStream{}
			_, err := r.ackMessage(outgoing)
			require.NoError(t, err)
			require.Len(t, outgoing.requests, 1)
			require.Equal(t, []string{"namespace"}, outgoing.requests[0].GetSyncReplicationState().GetThrottleHighNamespaceIds())
		})
	}
}
