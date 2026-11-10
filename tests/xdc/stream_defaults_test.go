package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type defaultStreamSuite struct {
	xdcBaseSuite
	tiered bool
}

func TestDefaultStreamImplementation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		transitions bool
		tiered      bool
	}{
		{name: "EventsSingleStack"},
		{name: "EventsTieredStack", tiered: true},
		{name: "StateSingleStack", transitions: true},
		{name: "StateTieredStack", transitions: true, tiered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &defaultStreamSuite{tiered: tc.tiered}
			s.enableTransitionHistory = tc.transitions
			suite.Run(t, s)
		})
	}
}

func (s *defaultStreamSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():               true,
		dynamicconfig.EnableReplicationTaskBatching.Key():         true,
		dynamicconfig.EnableSeparateReplicationEnableFlag.Key():   true,
		dynamicconfig.EnableReplicationTaskTieredProcessing.Key(): s.tiered,
	}
	s.setupSuite(testcore.WithReplicationStreamRecorder(), testcore.WithDCRedirectionPolicy(config.DCRedirectionPolicy{Policy: "noop"}))
}

func (s *defaultStreamSuite) SetupTest()     { s.setupTest() }
func (s *defaultStreamSuite) TearDownSuite() { s.tearDownSuite() }

func (s *defaultStreamSuite) TestLegacyReplication() {
	t := s.T()
	tv := testvars.New(t)
	require.Len(t, s.clusters, 2)
	captures := make([]*metricstest.Capture, len(s.clusters))
	for i, cluster := range s.clusters {
		handler, ok := cluster.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
		require.True(t, ok)
		captures[i] = handler.StartCapture()
		t.Cleanup(func() { handler.StopCapture(captures[i]) })
		cluster.GetReplicationStreamRecorder().Clear()
	}

	waitForStreams := func() {
		await.Require(t.Context(), t, func(t *await.T) {
			for _, capture := range captures {
				selected := make(map[string]bool)
				for _, recording := range capture.SnapshotMetric(metrics.ReplicationStreamImplementationSelected.Name()) {
					require.Equal(t, "legacy", recording.Tags["replication_stream_implementation"])
					selected[recording.Tags["operation"]] = true
				}
				require.True(t, selected[laneTestSenderOperation])
				require.True(t, selected[laneTestReceiverOperation])
			}
		}, 30*time.Second, 100*time.Millisecond)
		// Observe another ACK after both endpoints have selected replacement streams.
		ackStart := make([]int, len(s.clusters))
		for i, cluster := range s.clusters {
			ackStart[i] = len(cluster.GetReplicationStreamRecorder().GetMessages())
		}
		await.Require(t.Context(), t, func(t *await.T) {
			for i, cluster := range s.clusters {
				var acknowledged bool
				messages := cluster.GetReplicationStreamRecorder().GetMessages()
				require.GreaterOrEqual(t, len(messages), ackStart[i])
				for _, captured := range messages[ackStart[i]:] {
					for _, message := range []proto.Message{captured.Request, captured.Response} {
						request, ok := message.(interface {
							GetSyncReplicationState() *replicationspb.SyncReplicationState
						})
						if ok && request.GetSyncReplicationState() != nil {
							acknowledged = true
						}
					}
				}
				require.True(t, acknowledged, "must establish replication before creating work")
			}
		}, 30*time.Second, 100*time.Millisecond)
	}
	reload := func() {
		for i, cluster := range s.clusters {
			handler := cluster.Host().GetMetricsHandler().(*metricstest.CaptureHandler)
			handler.StopCapture(captures[i])
			captures[i] = handler.StartCapture()
		}
		for _, cluster := range s.clusters {
			for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
				_, err := cluster.AdminClient().CloseShard(t.Context(), &adminservice.CloseShardRequest{ShardId: shardID})
				require.NoError(t, err)
			}
		}
		// Streams are created only for loaded shards, including on the passive.
		for _, cluster := range s.clusters {
			for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
				_, err := cluster.AdminClient().DescribeHistoryHost(t.Context(), &adminservice.DescribeHistoryHostRequest{ShardId: shardID})
				require.NoError(t, err)
			}
		}
		waitForStreams()
	}
	reload()

	runs := make([]struct {
		namespace string
		execution *commonpb.WorkflowExecution
	}, len(s.clusters))
	for sourceIndex, active := range s.clusters {
		passive := s.clusters[1-sourceIndex]
		namespace := tv.Sub(active.ClusterName()).NamespaceName().String()
		_, err := active.FrontendClient().RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
			Namespace: namespace, Clusters: s.clusterReplicationConfig(), ActiveClusterName: active.ClusterName(),
			IsGlobalNamespace: true, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
		require.NoError(t, err)
		await.Require(t.Context(), t, func(t *await.T) {
			_, err := passive.FrontendClient().DescribeNamespace(t.Context(), &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
			require.NoError(t, err)
		}, 15*time.Second, 100*time.Millisecond)
		started, err := active.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
			Namespace: namespace, RequestId: tv.RequestID(), WorkflowId: tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		})
		require.NoError(t, err)
		runs[sourceIndex].namespace = namespace
		runs[sourceIndex].execution = &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: started.GetRunId()}
	}
	for phaseIndex, phase := range []string{"before-reload", "after-reload"} {
		if phase == "after-reload" {
			reload()
		}
		for sourceIndex, run := range runs {
			active, passive := s.clusters[sourceIndex], s.clusters[1-sourceIndex]
			history := func(ctx context.Context, client workflowservice.WorkflowServiceClient) (*historypb.History, error) {
				response, err := client.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace: run.namespace, Execution: run.execution,
				})
				if err != nil {
					return nil, err
				}
				return response.GetHistory(), nil
			}
			_, err := active.FrontendClient().SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: run.namespace, WorkflowExecution: run.execution,
				SignalName: phase, RequestId: tv.Sub(phase).RequestID(),
			})
			require.NoError(t, err)
			source, err := history(t.Context(), active.FrontendClient())
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(source.GetEvents()), 3+phaseIndex)
			await.Require(t.Context(), t, func(t *await.T) {
				target, err := history(t.Context(), passive.FrontendClient())
				require.NoError(t, err)
				protorequire.ProtoEqual(t, source, target)
			}, 30*time.Second, 100*time.Millisecond)
		}
	}

	await.Require(t.Context(), t, func(t *await.T) {
		for _, cluster := range s.clusters {
			var batches, acks int
			for _, captured := range cluster.GetReplicationStreamRecorder().GetMessages() {
				for _, message := range []proto.Message{captured.Request, captured.Response} {
					switch message := message.(type) {
					case interface {
						GetMessages() *replicationspb.WorkflowReplicationMessages
					}:
						batch := message.GetMessages()
						if len(batch.GetReplicationTasks()) > 0 {
							batches++
						}
						require.Nil(t, batch.GetLaneInfo())
						require.Empty(t, batch.GetIsolatedNamespaceId())
						require.False(t, batch.GetRetireIsolatedLane())
					case interface {
						GetSyncReplicationState() *replicationspb.SyncReplicationState
					}:
						state := message.GetSyncReplicationState()
						if state == nil {
							continue
						}
						acks++
						require.Equal(t, s.tiered, state.GetSupportsNamespaceIsolation())
						require.Empty(t, state.GetIsolatedLaneStates())
						require.Empty(t, state.GetThrottleHighNamespaceIds())
						require.Empty(t, state.GetLaneStates())
						require.Zero(t, state.GetLaneProtocolVersion())
						require.Zero(t, state.GetMaxLanes())
						require.Nil(t, state.GetLaneFeedback())
					default:
						continue
					}
				}
			}
			require.Positive(t, batches, "must observe real replication batches")
			require.Positive(t, acks, "must observe real replication acknowledgments")
			for shardID := int32(1); shardID <= s.numHistoryShards; shardID++ {
				shard, err := cluster.AdminClient().GetShard(t.Context(), &adminservice.GetShardRequest{ShardId: shardID})
				require.NoError(t, err)
				readers := shard.GetShardInfo().GetQueueStates()[int32(tasks.CategoryReplication.ID())].GetReaderStates()
				require.NotEmpty(t, readers, "must inspect an actual persisted replication reader")
				for _, reader := range readers {
					require.Empty(t, reader.GetLanes())
					require.Nil(t, reader.GetReplicationLaneDefaultCursor())
				}
			}
		}
	}, 30*time.Second, 100*time.Millisecond)
}
