// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"fmt"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination db_task_queue_ownership_mock.go

const (
	dbTaskQueueOwnershipStatusUninitialized dbTaskQueueOwnershipStatus = 0
	dbTaskQueueOwnershipStatusOwned         dbTaskQueueOwnershipStatus = 1
	dbTaskQueueOwnershipStatusLost          dbTaskQueueOwnershipStatus = 2
)

type (
	dbTaskQueueOwnershipStatus int

	dbTaskQueueOwnership interface {
		takeTaskQueueOwnership(ctx context.Context) error
		getShutdownChan() <-chan struct{}
		getAckedTaskID() int64
		updateAckedTaskID(taskID int64)
		getLastAllocatedTaskID() int64
		persistTaskQueue(ctx context.Context) error
		flushTasks(ctx context.Context, taskInfos ...*persistencespb.TaskInfo) error
	}

	dbTaskQueueOwnershipState struct {
		rangeID             int64
		ackedTaskID         int64
		lastAllocatedTaskID int64
		minTaskIDExclusive  int64 // exclusive
		maxTaskIDInclusive  int64 // inclusive
	}

	dbTaskQueueOwnershipImpl struct {
		taskQueueKey    persistence.TaskQueueKey
		taskQueueKind   enumspb.TaskQueueKind
		taskIDRangeSize int64
		timeSource      clock.TimeSource
		store           persistence.TaskManager
		logger          log.Logger

		sync.Mutex
		status              dbTaskQueueOwnershipStatus
		ownershipState      *dbTaskQueueOwnershipState
		shutdownChan        chan struct{}
		stateLastUpdateTime *time.Time
	}
)

func newDBTaskQueueOwnership(
	taskQueueKey persistence.TaskQueueKey,
	taskQueueKind enumspb.TaskQueueKind,
	taskIDRangeSize int64,
	store persistence.TaskManager,
	logger log.Logger,
) *dbTaskQueueOwnershipImpl {
	taskOwnership := &dbTaskQueueOwnershipImpl{
		taskQueueKey:    taskQueueKey,
		taskQueueKind:   taskQueueKind,
		taskIDRangeSize: taskIDRangeSize,
		timeSource:      clock.NewRealTimeSource(),
		store:           store,
		logger:          logger,

		status:              dbTaskQueueOwnershipStatusUninitialized,
		ownershipState:      nil,
		shutdownChan:        make(chan struct{}),
		stateLastUpdateTime: nil,
	}
	return taskOwnership
}

func (m *dbTaskQueueOwnershipImpl) getShutdownChan() <-chan struct{} {
	return m.shutdownChan
}

func (m *dbTaskQueueOwnershipImpl) getAckedTaskID() int64 {
	m.Lock()
	defer m.Unlock()

	return m.ownershipState.ackedTaskID
}

func (m *dbTaskQueueOwnershipImpl) updateAckedTaskID(taskID int64) {
	m.Lock()
	defer m.Unlock()

	if m.ownershipState.ackedTaskID >= taskID {
		return
	}
	m.ownershipState.ackedTaskID = taskID
}

func (m *dbTaskQueueOwnershipImpl) getLastAllocatedTaskID() int64 {
	m.Lock()
	defer m.Unlock()

	return m.ownershipState.lastAllocatedTaskID
}

func (m *dbTaskQueueOwnershipImpl) takeTaskQueueOwnership(
	ctx context.Context,
) error {
	m.Lock()
	defer m.Unlock()

	response, err := m.store.GetTaskQueue(ctx, &persistence.GetTaskQueueRequest{
		NamespaceID: m.taskQueueKey.NamespaceID,
		TaskQueue:   m.taskQueueKey.TaskQueueName,
		TaskType:    m.taskQueueKey.TaskQueueType,
	})
	switch err.(type) {
	case nil:
		m.updateStateLocked(response.RangeID, response.TaskQueueInfo.AckLevel)
		if err := m.renewTaskQueueLocked(ctx, response.RangeID+1); err != nil {
			return err
		}
		m.status = dbTaskQueueOwnershipStatusOwned
		return nil

	case *serviceerror.NotFound:
		if _, err := m.store.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
			RangeID: dbTaskInitialRangeID,
			TaskQueueInfo: &persistencespb.TaskQueueInfo{
				NamespaceId:    m.taskQueueKey.NamespaceID,
				Name:           m.taskQueueKey.TaskQueueName,
				TaskType:       m.taskQueueKey.TaskQueueType,
				Kind:           m.taskQueueKind,
				AckLevel:       0,
				ExpiryTime:     m.expiryTime(),
				LastUpdateTime: timestamp.TimePtr(m.timeSource.Now()),
			},
		}); err != nil {
			m.maybeShutdownLocked(err)
			return err
		}
		m.stateLastUpdateTime = timestamp.TimePtr(m.timeSource.Now())
		m.updateStateLocked(dbTaskInitialRangeID, 0)
		m.status = dbTaskQueueOwnershipStatusOwned
		return nil

	default:
		return err
	}
}

func (m *dbTaskQueueOwnershipImpl) renewTaskQueueLocked(
	ctx context.Context,
	rangeID int64,
) error {
	_, err := m.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: m.taskQueueInfoLocked(),
		PrevRangeID:   m.ownershipState.rangeID,
	})
	if err != nil {
		m.maybeShutdownLocked(err)
		return err
	}
	m.stateLastUpdateTime = timestamp.TimePtr(m.timeSource.Now())
	m.updateStateLocked(rangeID, m.ownershipState.ackedTaskID)
	return nil
}

func (m *dbTaskQueueOwnershipImpl) persistTaskQueue(
	ctx context.Context,
) error {
	m.Lock()
	defer m.Unlock()

	return m.renewTaskQueueLocked(ctx, m.ownershipState.rangeID)
}

func (m *dbTaskQueueOwnershipImpl) flushTasks(
	ctx context.Context,
	taskInfos ...*persistencespb.TaskInfo,
) error {
	m.Lock()
	defer m.Unlock()

	taskIDs, err := m.generatedTaskIDsLocked(ctx, len(taskInfos))
	if err != nil {
		return err
	}

	allocatedTaskInfos := make([]*persistencespb.AllocatedTaskInfo, len(taskInfos))
	for i, taskID := range taskIDs {
		allocatedTaskInfos[i] = &persistencespb.AllocatedTaskInfo{
			Data:   taskInfos[i],
			TaskId: taskID,
		}
	}
	_, err = m.store.CreateTasks(ctx, &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data:    m.taskQueueInfoLocked(),
			RangeID: m.ownershipState.rangeID,
		},
		Tasks: allocatedTaskInfos,
	})
	if err != nil {
		m.maybeShutdownLocked(err)
		return err
	}
	m.stateLastUpdateTime = timestamp.TimePtr(m.timeSource.Now())
	return nil
}

func (m *dbTaskQueueOwnershipImpl) generatedTaskIDsLocked(
	ctx context.Context,
	numTasks int,
) ([]int64, error) {
	if m.ownershipState.maxTaskIDInclusive-m.ownershipState.lastAllocatedTaskID < int64(numTasks) {
		if err := m.renewTaskQueueLocked(ctx, m.ownershipState.rangeID+1); err != nil {
			return nil, err
		}
	}
	if m.ownershipState.maxTaskIDInclusive-m.ownershipState.lastAllocatedTaskID < int64(numTasks) {
		panic("dbTaskQueueOwnershipImpl generatedTaskIDsLocked unable to allocate task IDs")
	}

	allocatedTaskIDs := make([]int64, numTasks)
	for i := 0; i < numTasks; i++ {
		m.ownershipState.lastAllocatedTaskID++
		if m.ownershipState.lastAllocatedTaskID > m.ownershipState.maxTaskIDInclusive {
			panic("dbTaskQueueOwnershipImpl generatedTaskIDsLocked encountered task ID overflow")
		}
		allocatedTaskIDs[i] = m.ownershipState.lastAllocatedTaskID
	}
	return allocatedTaskIDs, nil
}

func (m *dbTaskQueueOwnershipImpl) taskQueueInfoLocked() *persistencespb.TaskQueueInfo {
	return &persistencespb.TaskQueueInfo{
		NamespaceId:    m.taskQueueKey.NamespaceID,
		Name:           m.taskQueueKey.TaskQueueName,
		TaskType:       m.taskQueueKey.TaskQueueType,
		Kind:           m.taskQueueKind,
		AckLevel:       m.ownershipState.ackedTaskID,
		ExpiryTime:     m.expiryTime(),
		LastUpdateTime: timestamp.TimePtr(m.timeSource.Now()),
	}
}

func (m *dbTaskQueueOwnershipImpl) expiryTime() *time.Time {
	switch m.taskQueueKind {
	case enumspb.TASK_QUEUE_KIND_NORMAL:
		return nil
	case enumspb.TASK_QUEUE_KIND_STICKY:
		return timestamp.TimePtr(m.timeSource.Now().Add(dbTaskStickyTaskQueueTTL))
	default:
		panic(fmt.Sprintf("taskQueueDB encountered unknown task kind: %v", m.taskQueueKind))
	}
}

func (m *dbTaskQueueOwnershipImpl) updateStateLocked(
	rangeID int64,
	ackedTaskID int64,
) {
	minTaskID, maxTaskID := rangeIDToTaskIDRange(rangeID, m.taskIDRangeSize)
	if m.ownershipState == nil {
		m.ownershipState = &dbTaskQueueOwnershipState{
			rangeID:             rangeID,
			ackedTaskID:         ackedTaskID,
			lastAllocatedTaskID: minTaskID,
			minTaskIDExclusive:  minTaskID,
			maxTaskIDInclusive:  maxTaskID,
		}
	} else {
		if rangeID < m.ownershipState.rangeID {
			panic("dbTaskQueueOwnershipImpl updateStateLocked encountered smaller range ID")
		} else if ackedTaskID < m.ownershipState.ackedTaskID {
			panic("dbTaskQueueOwnershipImpl updateStateLocked encountered acked task ID")
		}
		m.ownershipState.rangeID = rangeID
		m.ownershipState.ackedTaskID = ackedTaskID
		if minTaskID > m.ownershipState.lastAllocatedTaskID {
			m.ownershipState.lastAllocatedTaskID = minTaskID
		}
		m.ownershipState.minTaskIDExclusive = minTaskID
		m.ownershipState.maxTaskIDInclusive = maxTaskID
	}
}

func (m *dbTaskQueueOwnershipImpl) maybeShutdownLocked(
	err error,
) {
	_, ok := err.(*persistence.ConditionFailedError)
	if !ok {
		return
	}

	m.ownershipState = nil
	if m.status == dbTaskQueueOwnershipStatusLost {
		return
	}
	m.status = dbTaskQueueOwnershipStatusLost
	close(m.shutdownChan)
}

func rangeIDToTaskIDRange(
	rangeID int64,
	taskIDRangeSize int64,
) (int64, int64) {
	return (rangeID - 1) * taskIDRangeSize, rangeID * taskIDRangeSize
}
