package matching

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqid"
)

func (m *pollMetadata) isSingleSessionAcquisition() bool {
	return m != nil && m.sessionOptions.GetSingleSession() != nil && m.forwardedFrom != ""
}

func (pm *taskQueuePartitionManagerImpl) pollSingleSession(
	ctx context.Context,
	queue physicalTaskQueueManager,
	metadata *pollMetadata,
) (*internalTask, error) {
	pm.singleSessionAcquisitionMutex.Lock()
	if pm.singleSessionAcquisitionInProgress {
		pm.singleSessionAcquisitionMutex.Unlock()
		queue.MarkAlive()
		return nil, errNoTasks
	}
	if queue.SingleSessionAssigned() {
		pm.singleSessionAcquisitionMutex.Unlock()
		return queue.PollTask(ctx, metadata)
	}
	if ctx.Err() != nil {
		pm.singleSessionAcquisitionMutex.Unlock()
		return nil, errNoTasks
	}
	if pm.partition.TaskQueue().Name() == "" {
		pm.singleSessionAcquisitionMutex.Unlock()
		return nil, serviceerror.NewInvalidArgument("single-session workflow polls require a normal task queue name")
	}
	pm.singleSessionAcquisitionInProgress = true
	pm.singleSessionAcquisitionMutex.Unlock()

	task, err := pm.acquireSingleSession(ctx, queue, metadata)
	// A failed RPC may have started a workflow; only a confirmed empty poll permits another acquisition.
	if err == nil || errors.Is(err, errNoTasks) {
		pm.singleSessionAcquisitionMutex.Lock()
		pm.singleSessionAcquisitionInProgress = false
		pm.singleSessionAcquisitionMutex.Unlock()
	}
	return task, err
}

func (pm *taskQueuePartitionManagerImpl) acquireSingleSession(
	ctx context.Context,
	queue physicalTaskQueueManager,
	metadata *pollMetadata,
) (*internalTask, error) {
	queue.MarkAlive()
	task, err := ForwardPollWithTarget(ctx, metadata, pm.matchingClient, pm.partition, pm.partition.TaskQueue().RootPartition())
	if err != nil {
		return nil, err
	}
	if err := queue.AssignSingleSession(); err != nil {
		return nil, err
	}
	task.namespace = pm.ns.Name()
	return task, nil
}

func (c *physicalTaskQueueManagerImpl) SingleSessionAssigned() bool {
	return c.backlogMgr.getDB().SingleSessionAssigned()
}

func (c *physicalTaskQueueManagerImpl) AssignSingleSession() error {
	ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
	defer cancel()
	changed, err := c.backlogMgr.getDB().assignSingleSession(ctx)
	if changed {
		// Assignment advances the lease fence; reload to give the task writer a consistent ID block.
		c.UnloadFromPartitionManager(unloadCauseUnspecified)
	}
	return err
}

func (db *taskQueueDB) SingleSessionAssigned() bool {
	db.Lock()
	defer db.Unlock()
	return db.singleSessionAssigned
}

func (db *taskQueueDB) assignSingleSession(ctx context.Context) (bool, error) {
	db.Lock()
	defer db.Unlock()
	if db.singleSessionAssigned {
		return false, nil
	}
	db.singleSessionAssigned = true
	db.lastChange = time.Now()
	// Fence scanner snapshots captured before this record became retained metadata.
	return true, db.updateTaskQueueLocked(ctx, true)
}

func (e *matchingEngineImpl) singleSessionAssigned(ctx context.Context, partition tqid.Partition) (bool, error) {
	queue := UnversionedQueueKey(partition)
	response, err := e.taskManager.GetTaskQueue(ctx, &persistence.GetTaskQueueRequest{
		NamespaceID: queue.NamespaceId(),
		TaskQueue:   queue.PersistenceName(),
		TaskType:    queue.TaskType(),
	})
	if _, notFound := errors.AsType[*serviceerror.NotFound](err); notFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return persistence.IsTaskQueueMetadataRetained(response.TaskQueueInfo), nil
}
