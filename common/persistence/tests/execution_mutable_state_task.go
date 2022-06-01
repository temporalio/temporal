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

package tests

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

type (
	ExecutionMutableStateTaskSuite struct {
		suite.Suite
		*require.Assertions

		ShardID     int32
		RangeID     int64
		WorkflowKey definition.WorkflowKey

		ShardManager     p.ShardManager
		ExecutionManager p.ExecutionManager
		Logger           log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewExecutionMutableStateTaskSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	serializer serialization.Serializer,
	logger log.Logger,
) *ExecutionMutableStateTaskSuite {
	return &ExecutionMutableStateTaskSuite{
		Assertions: require.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		ExecutionManager: p.NewExecutionManager(
			executionStore,
			serializer,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		Logger: logger,
	}
}

func (s *ExecutionMutableStateTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), time.Second*30)

	s.ShardID = 1 + rand.Int31n(maxShards)
	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID: s.ShardID,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.ShardID,
			RangeId: 1,
		},
	})
	s.NoError(err)
	previousRangeID := resp.ShardInfo.RangeId
	resp.ShardInfo.RangeId += 1
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       resp.ShardInfo,
		PreviousRangeID: previousRangeID,
	})
	s.NoError(err)
	s.RangeID = resp.ShardInfo.RangeId

	s.WorkflowKey = definition.NewWorkflowKey(
		uuid.New().String(),
		uuid.New().String(),
		uuid.New().String(),
	)

}

func (s *ExecutionMutableStateTaskSuite) TearDownTest() {
	for _, category := range []tasks.Category{tasks.CategoryTransfer, tasks.CategoryReplication, tasks.CategoryVisibility} {
		err := s.ExecutionManager.RangeCompleteHistoryTasks(s.Ctx, &p.RangeCompleteHistoryTasksRequest{
			ShardID:             s.ShardID,
			TaskCategory:        category,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(math.MaxInt64),
		})
		s.NoError(err)
	}
	err := s.ExecutionManager.RangeCompleteHistoryTasks(s.Ctx, &p.RangeCompleteHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(time.Unix(0, 0), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(time.Unix(0, math.MaxInt64), 0),
	})
	s.NoError(err)

	s.Cancel()
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetTransferTasks_Multiple() {
	numTasks := 20
	transferTasks := s.AddRandomTasks(
		tasks.CategoryTransfer,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.ActivityTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)

	transferTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(transferTasks)
	loadedTasks := s.PaginateTasks(
		tasks.CategoryTransfer,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(transferTasks)*2)+1,
	)
	s.Equal(transferTasks, loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetTimerTasks_Multiple() {
	numTasks := 20
	timerTasks := s.AddRandomTasks(
		tasks.CategoryTimer,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.UserTimerTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)

	timerTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(timerTasks)
	loadedTasks := s.PaginateTasks(
		tasks.CategoryTimer,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(timerTasks)*2)+1,
	)
	s.Equal(timerTasks, loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetReplicationTasks_Multiple() {
	numTasks := 20
	replicationTasks := s.AddRandomTasks(
		tasks.CategoryReplication,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.HistoryReplicationTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)

	replicationTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(replicationTasks)
	loadedTasks := s.PaginateTasks(
		tasks.CategoryReplication,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(replicationTasks)*2)+1,
	)
	s.Equal(replicationTasks, loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetVisibilityTasks_Multiple() {
	numTasks := 20
	visibilityTasks := s.AddRandomTasks(
		tasks.CategoryVisibility,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.StartExecutionVisibilityTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)

	visibilityTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(visibilityTasks)
	loadedTasks := s.PaginateTasks(
		tasks.CategoryVisibility,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(visibilityTasks)*2)+1,
	)
	s.Equal(visibilityTasks, loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) AddRandomTasks(
	category tasks.Category,
	numTasks int,
	newTaskFn func(definition.WorkflowKey, int64, time.Time) tasks.Task,
) []tasks.Task {
	currentTaskID := int64(1)
	now := time.Now().UTC()
	randomTasks := make([]tasks.Task, 0, numTasks)
	for i := 0; i != numTasks; i++ {
		randomTasks = append(randomTasks, newTaskFn(s.WorkflowKey, currentTaskID, now))
		currentTaskID += rand.Int63n(100) + 1
		now = now.Add(time.Duration(rand.Int63n(1000_000_000)) + time.Millisecond)
	}

	err := s.ExecutionManager.AddHistoryTasks(s.Ctx, &p.AddHistoryTasksRequest{
		ShardID:     s.ShardID,
		RangeID:     s.RangeID,
		NamespaceID: s.WorkflowKey.NamespaceID,
		WorkflowID:  s.WorkflowKey.WorkflowID,
		RunID:       s.WorkflowKey.RunID,
		Tasks: map[tasks.Category][]tasks.Task{
			category: randomTasks,
		},
	})
	s.NoError(err)

	return randomTasks
}

func (s *ExecutionMutableStateTaskSuite) PaginateTasks(
	category tasks.Category,
	inclusiveMinTaskKey tasks.Key,
	exclusiveMaxTaskKey tasks.Key,
	batchSize int,
) []tasks.Task {
	request := &p.GetHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        category,
		InclusiveMinTaskKey: inclusiveMinTaskKey,
		ExclusiveMaxTaskKey: exclusiveMaxTaskKey,
		BatchSize:           batchSize,
	}
	var loadedTasks []tasks.Task
	for {
		response, err := s.ExecutionManager.GetHistoryTasks(s.Ctx, request)
		s.NoError(err)
		s.True(len(response.Tasks) <= batchSize)
		loadedTasks = append(loadedTasks, response.Tasks...)
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}
	return loadedTasks
}

func (s *ExecutionMutableStateTaskSuite) RandomPaginateRange(
	createdTasks []tasks.Task,
) ([]tasks.Task, tasks.Key, tasks.Key) {
	numTasks := len(createdTasks)
	firstTaskIdx := rand.Intn(numTasks/2 - 1)
	nextTaskIdx := firstTaskIdx + rand.Intn(numTasks/2-1) + 1

	inclusiveMinTaskKey := createdTasks[firstTaskIdx].GetKey()
	var exclusiveMaxTaskKey tasks.Key
	if nextTaskIdx == numTasks {
		exclusiveMaxTaskKey = tasks.NewKey(
			createdTasks[numTasks-1].GetVisibilityTime().Add(time.Second),
			createdTasks[numTasks-1].GetTaskID()+10,
		)
	} else {
		exclusiveMaxTaskKey = createdTasks[nextTaskIdx].GetKey()
	}

	taskCategory := createdTasks[0].GetCategory()
	switch taskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		inclusiveMinTaskKey.FireTime = tasks.DefaultFireTime
		exclusiveMaxTaskKey.FireTime = tasks.DefaultFireTime
	case tasks.CategoryTypeScheduled:
		inclusiveMinTaskKey.TaskID = 0
		exclusiveMaxTaskKey.TaskID = 0
	}

	return createdTasks[firstTaskIdx:nextTaskIdx], inclusiveMinTaskKey, exclusiveMaxTaskKey
}
