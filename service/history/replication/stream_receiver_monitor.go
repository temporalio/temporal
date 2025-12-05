package replication

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	streamReceiverMonitorInterval = 2 * time.Second
)

type (
	StreamReceiverMonitor interface {
		RegisterInboundStream(streamSender StreamSender)
		Start()
		Stop()
	}
	StreamReceiverMonitorImpl struct {
		ProcessToolBox
		executableTaskConverter ExecutableTaskConverter
		enableStreaming         bool

		status       int32
		shutdownOnce channel.ShutdownOnce

		sync.Mutex
		inboundStreams  map[ClusterShardKeyPair]StreamSender
		outboundStreams map[ClusterShardKeyPair]StreamReceiver
		previousStatus  map[ClusterShardKeyPair]*streamStatus
	}
	streamStatus struct {
		defaultAckLevel      int64
		highPriorityAckLevel int64
		lowPriorityAckLevel  int64
		maxReplicationTaskId int64
		isTieredStackEnabled bool
	}
)

func NewStreamReceiverMonitor(
	processToolBox ProcessToolBox,
	executableTaskConverter ExecutableTaskConverter,
	enableStreaming bool,
) *StreamReceiverMonitorImpl {
	return &StreamReceiverMonitorImpl{
		ProcessToolBox:          processToolBox,
		executableTaskConverter: executableTaskConverter,
		enableStreaming:         enableStreaming,

		status:       streamStatusInitialized,
		shutdownOnce: channel.NewShutdownOnce(),

		inboundStreams:  make(map[ClusterShardKeyPair]StreamSender),
		outboundStreams: make(map[ClusterShardKeyPair]StreamReceiver),
	}
}

func (m *StreamReceiverMonitorImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	if !m.enableStreaming {
		return
	}

	go m.eventLoop()
	go m.statusMonitorLoop()

	m.Logger.Info("StreamReceiverMonitor started.")
}

func (m *StreamReceiverMonitorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	if !m.enableStreaming {
		return
	}

	m.shutdownOnce.Shutdown()
	m.Lock()
	defer m.Unlock()
	for serverKey, stream := range m.outboundStreams {
		stream.Stop()
		delete(m.outboundStreams, serverKey)
	}
	m.Logger.Info("StreamReceiverMonitor stopped.")
}

func (m *StreamReceiverMonitorImpl) RegisterInboundStream(
	streamSender StreamSender,
) {
	streamKey := streamSender.Key()

	m.Lock()
	defer m.Unlock()

	if staleSender, ok := m.inboundStreams[streamKey]; ok {
		staleSender.Stop()
		delete(m.inboundStreams, streamKey)
	}
	m.inboundStreams[streamKey] = streamSender
}

func (m *StreamReceiverMonitorImpl) eventLoop() {
	defer m.Stop()
	ticker := time.NewTicker(streamReceiverMonitorInterval)
	defer ticker.Stop()

	clusterMetadataChangeChan := make(chan struct{}, 1)
	m.ClusterMetadata.RegisterMetadataChangeCallback(m, func(_ map[string]*cluster.ClusterInformation, _ map[string]*cluster.ClusterInformation) {
		select {
		case clusterMetadataChangeChan <- struct{}{}:
		default:
		}
	})
	defer m.ClusterMetadata.UnRegisterMetadataChangeCallback(m)
	m.reconcileOutboundStreams()

Loop:
	for !m.shutdownOnce.IsShutdown() {
		select {
		case <-clusterMetadataChangeChan:
			m.reconcileInboundStreams()
			m.reconcileOutboundStreams()
		case <-ticker.C:
			m.reconcileInboundStreams()
			m.reconcileOutboundStreams()
		case <-m.shutdownOnce.Channel():
			break Loop
		}
	}
}

func (m *StreamReceiverMonitorImpl) reconcileInboundStreams() {
	streamKeys := m.generateInboundStreamKeys()
	m.doReconcileInboundStreams(streamKeys)
}

func (m *StreamReceiverMonitorImpl) reconcileOutboundStreams() {
	streamKeys := m.generateOutboundStreamKeys()
	m.doReconcileOutboundStreams(streamKeys)
}

func (m *StreamReceiverMonitorImpl) generateInboundStreamKeys() map[ClusterShardKeyPair]struct{} {
	allClusterInfo := m.ClusterMetadata.GetAllClusterInfo()

	clientClusterIDs := make(map[int32]struct{})
	serverClusterID := int32(m.ClusterMetadata.GetClusterID())
	clusterIDToShardCount := make(map[int32]int32)
	for _, clusterInfo := range allClusterInfo {
		clusterIDToShardCount[int32(clusterInfo.InitialFailoverVersion)] = clusterInfo.ShardCount

		if !cluster.IsReplicationEnabledForCluster(clusterInfo, m.Config.EnableSeparateReplicationEnableFlag()) || int32(clusterInfo.InitialFailoverVersion) == serverClusterID {
			continue
		}
		clientClusterIDs[int32(clusterInfo.InitialFailoverVersion)] = struct{}{}
	}
	streamKeys := make(map[ClusterShardKeyPair]struct{})
	for _, shardID := range m.ShardController.ShardIDs() {
		for clientClusterID := range clientClusterIDs {
			serverShardID := shardID
			for _, clientShardID := range common.MapShardID(
				clusterIDToShardCount[serverClusterID],
				clusterIDToShardCount[clientClusterID],
				serverShardID,
			) {
				m.Logger.Debug(fmt.Sprintf(
					"inbound cluster shard ID %v/%v -> cluster shard ID %v/%v",
					clientClusterID, clientShardID, serverClusterID, serverShardID,
				))
				streamKeys[ClusterShardKeyPair{
					Client: NewClusterShardKey(clientClusterID, clientShardID),
					Server: NewClusterShardKey(serverClusterID, serverShardID),
				}] = struct{}{}
			}
		}
	}
	return streamKeys
}

func (m *StreamReceiverMonitorImpl) generateOutboundStreamKeys() map[ClusterShardKeyPair]struct{} {
	allClusterInfo := m.ClusterMetadata.GetAllClusterInfo()

	clientClusterID := int32(m.ClusterMetadata.GetClusterID())
	serverClusterIDs := make(map[int32]struct{})
	clusterIDToShardCount := make(map[int32]int32)
	for _, clusterInfo := range allClusterInfo {
		clusterIDToShardCount[int32(clusterInfo.InitialFailoverVersion)] = clusterInfo.ShardCount

		if !clusterInfo.Enabled || !cluster.IsReplicationEnabledForCluster(clusterInfo, m.Config.EnableSeparateReplicationEnableFlag()) || int32(clusterInfo.InitialFailoverVersion) == clientClusterID {
			continue
		}
		serverClusterIDs[int32(clusterInfo.InitialFailoverVersion)] = struct{}{}
	}
	streamKeys := make(map[ClusterShardKeyPair]struct{})
	for _, shardID := range m.ShardController.ShardIDs() {
		for serverClusterID := range serverClusterIDs {
			clientShardID := shardID
			for _, serverShardID := range common.MapShardID(
				clusterIDToShardCount[clientClusterID],
				clusterIDToShardCount[serverClusterID],
				clientShardID,
			) {
				m.Logger.Debug(fmt.Sprintf(
					"outbound cluster shard ID %v/%v -> cluster shard ID %v/%v",
					clientClusterID, clientShardID, serverClusterID, serverShardID,
				))
				streamKeys[ClusterShardKeyPair{
					Client: NewClusterShardKey(clientClusterID, clientShardID),
					Server: NewClusterShardKey(serverClusterID, serverShardID),
				}] = struct{}{}
			}
		}
	}
	return streamKeys
}

func (m *StreamReceiverMonitorImpl) doReconcileInboundStreams(
	streamKeys map[ClusterShardKeyPair]struct{},
) {
	m.Lock()
	defer m.Unlock()
	if m.shutdownOnce.IsShutdown() {
		return
	}

	for streamKey, stream := range m.inboundStreams {
		if !stream.IsValid() {
			stream.Stop()
			delete(m.inboundStreams, streamKey)
		} else if _, ok := streamKeys[streamKey]; !ok {
			stream.Stop()
			delete(m.inboundStreams, streamKey)
		}
	}
}

func (m *StreamReceiverMonitorImpl) doReconcileOutboundStreams(
	streamKeys map[ClusterShardKeyPair]struct{},
) {
	m.Lock()
	defer m.Unlock()
	if m.shutdownOnce.IsShutdown() {
		return
	}

	for streamKey, stream := range m.outboundStreams {
		if !stream.IsValid() {
			stream.Stop()
			delete(m.outboundStreams, streamKey)
		} else if _, ok := streamKeys[streamKey]; !ok {
			stream.Stop()
			delete(m.outboundStreams, streamKey)
		}
	}
	for streamKey := range streamKeys {
		if _, ok := m.outboundStreams[streamKey]; !ok {
			stream := NewStreamReceiver(
				m.ProcessToolBox,
				m.executableTaskConverter,
				streamKey.Client,
				streamKey.Server,
			)
			stream.Start()
			m.outboundStreams[streamKey] = stream
		}
	}
}

func (m *StreamReceiverMonitorImpl) statusMonitorLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.monitorStreamStatus()
		case <-m.shutdownOnce.Channel():
			return
		}
	}
}
func (m *StreamReceiverMonitorImpl) monitorStreamStatus() {
	var panicErr error
	defer func() {
		log.CapturePanic(m.Logger, &panicErr)
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(m.MetricsHandler).Record(1)
		}
	}()

	if m.shutdownOnce.IsShutdown() {
		return
	}
	statusMap := m.generateStatusMap(m.generateInboundStreamKeys())
	if m.previousStatus == nil {
		m.previousStatus = statusMap
		return
	}
	m.evaluateStreamStatus(statusMap)
	m.previousStatus = statusMap
}

func (m *StreamReceiverMonitorImpl) evaluateStreamStatus(currentStatusMap map[ClusterShardKeyPair]*streamStatus) {
	for key, currentStatus := range currentStatusMap {
		m.evaluateSingleStreamConnection(&key, currentStatus, m.previousStatus[key])
	}
}

func (m *StreamReceiverMonitorImpl) evaluateSingleStreamConnection(key *ClusterShardKeyPair, current *streamStatus, previous *streamStatus) bool {
	if previous == nil || current == nil { // cluster metadata change or shard movement could cause stream reconnect and status could be nil
		return true
	}
	if previous.isTieredStackEnabled != current.isTieredStackEnabled { // there is tiered stack config change, wait until it becomes stable
		return true
	}
	checkIfMakeProgress := func(priority enumsspb.TaskPriority, currentAckLevel int64, currentMaxTaskId int64, previousAckLevel int64, previousMaxReplicationTaskId int64) bool {
		// 2 continuous data points where ACK level is not moving forward and ACK level is behind previous Max Replication taskId
		if currentAckLevel == previousAckLevel && currentAckLevel < previousMaxReplicationTaskId {
			m.Logger.Error(
				fmt.Sprintf("%v replication is not making progress. previousAckLevel: %v, previousMaxTaskId: %v, currentAckLevel: %v, currentMaxTaskId: %v",
					priority.String(), previousAckLevel, previousMaxReplicationTaskId, currentAckLevel, currentMaxTaskId),
				tag.SourceShardID(key.Server.ShardID), tag.TargetCluster(strconv.Itoa(int(key.Client.ClusterID))), tag.TargetShardID(key.Client.ShardID))
			metrics.ReplicationStreamStuck.With(m.MetricsHandler).Record(
				int64(1),
				metrics.FromClusterIDTag(key.Server.ClusterID),
				metrics.ToClusterIDTag(key.Client.ClusterID),
			)
			return false
		}
		return true
	}

	if !current.isTieredStackEnabled {
		return checkIfMakeProgress(enumsspb.TASK_PRIORITY_UNSPECIFIED, current.defaultAckLevel, current.maxReplicationTaskId, previous.defaultAckLevel, previous.maxReplicationTaskId)
	}
	highPriorityResult := checkIfMakeProgress(enumsspb.TASK_PRIORITY_HIGH, current.highPriorityAckLevel, current.maxReplicationTaskId, previous.highPriorityAckLevel, previous.maxReplicationTaskId)
	lowPriorityResult := checkIfMakeProgress(enumsspb.TASK_PRIORITY_LOW, current.lowPriorityAckLevel, current.maxReplicationTaskId, previous.lowPriorityAckLevel, previous.maxReplicationTaskId)
	return highPriorityResult && lowPriorityResult
}

func (m *StreamReceiverMonitorImpl) generateStatusMap(inboundKeys map[ClusterShardKeyPair]struct{}) map[ClusterShardKeyPair]*streamStatus {
	serverToClients := make(map[ClusterShardKey][]ClusterShardKey)
	for keyPair := range inboundKeys {
		serverToClients[keyPair.Server] = append(serverToClients[keyPair.Server], keyPair.Client)
	}
	statusMap := make(map[ClusterShardKeyPair]*streamStatus)
	for serverKey, clientKeys := range serverToClients {
		m.fillStatusMap(statusMap, serverKey, clientKeys)
	}
	return statusMap
}

func (m *StreamReceiverMonitorImpl) fillStatusMap(statusMap map[ClusterShardKeyPair]*streamStatus, serverKey ClusterShardKey, clientsKeys []ClusterShardKey) {
	shardContext, err := m.ShardController.GetShardByID(serverKey.ShardID)
	if err != nil {
		m.Logger.Error("Failed to get shardContext.", tag.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		m.Logger.Error("Failed to get engine.", tag.Error(err))
		return
	}
	maxTaskId, _ := engine.GetMaxReplicationTaskInfo()
	queueState, ok := shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		m.Logger.Error("Failed to get queue state.")
		return
	}
	readerStates := queueState.GetReaderStates()
	for _, clientKey := range clientsKeys {
		readerID := shard.ReplicationReaderIDFromClusterShardID(
			int64(clientKey.ClusterID),
			clientKey.ShardID,
		)
		readerState, ok := readerStates[readerID]
		if !ok {
			m.Logger.Error("Failed to get reader state.")
			statusMap[ClusterShardKeyPair{Client: clientKey, Server: serverKey}] = &streamStatus{
				maxReplicationTaskId: maxTaskId,
				isTieredStackEnabled: false,
			}
			continue
		}
		if len(readerState.Scopes) == 3 {
			statusMap[ClusterShardKeyPair{Client: clientKey, Server: serverKey}] = &streamStatus{
				defaultAckLevel:      readerState.Scopes[0].Range.InclusiveMin.TaskId,
				highPriorityAckLevel: readerState.Scopes[1].Range.InclusiveMin.TaskId,
				lowPriorityAckLevel:  readerState.Scopes[2].Range.InclusiveMin.TaskId,
				maxReplicationTaskId: maxTaskId,
				isTieredStackEnabled: true,
			}
		} else if len(readerState.Scopes) == 1 {
			statusMap[ClusterShardKeyPair{Client: clientKey, Server: serverKey}] = &streamStatus{
				defaultAckLevel:      readerState.Scopes[0].Range.InclusiveMin.TaskId,
				highPriorityAckLevel: 0,
				lowPriorityAckLevel:  0,
				maxReplicationTaskId: maxTaskId,
				isTieredStackEnabled: false,
			}
		}
	}
}
