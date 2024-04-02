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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
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
		Owner       string
		WorkflowKey definition.WorkflowKey

		ShardManager     p.ShardManager
		ExecutionManager p.ExecutionManager
		Logger           log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}

	testSerializer struct {
		serialization.Serializer
	}
)

var (
	fakeImmediateTaskCategory = tasks.NewCategory(1234, tasks.CategoryTypeImmediate, "fake-immediate")
	fakeScheduledTaskCategory = tasks.NewCategory(2345, tasks.CategoryTypeScheduled, "fake-scheduled")

	taskCategories = []tasks.Category{
		tasks.CategoryTransfer,
		tasks.CategoryTimer,
		tasks.CategoryReplication,
		tasks.CategoryVisibility,
		fakeImmediateTaskCategory,
		fakeScheduledTaskCategory,
	}
)

func NewExecutionMutableStateTaskSuite(
	t *testing.T,
	shardStore p.ShardStore,
	executionStore p.ExecutionStore,
	serializer serialization.Serializer,
	logger log.Logger,
) *ExecutionMutableStateTaskSuite {
	serializer = newTestSerializer(serializer)
	return &ExecutionMutableStateTaskSuite{
		Assertions: require.New(t),
		ShardManager: p.NewShardManager(
			shardStore,
			serializer,
		),
		ExecutionManager: p.NewExecutionManager(
			executionStore,
			serializer,
			nil,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		Logger: logger,
	}
}

func (s *ExecutionMutableStateTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.ShardID++
	resp, err := s.ShardManager.GetOrCreateShard(s.Ctx, &p.GetOrCreateShardRequest{
		ShardID: s.ShardID,
		InitialShardInfo: &persistencespb.ShardInfo{
			ShardId: s.ShardID,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
	})
	s.NoError(err)
	previousRangeID := resp.ShardInfo.RangeId
	resp.ShardInfo.RangeId++
	err = s.ShardManager.UpdateShard(s.Ctx, &p.UpdateShardRequest{
		ShardInfo:       resp.ShardInfo,
		PreviousRangeID: previousRangeID,
	})
	s.NoError(err)
	s.RangeID = resp.ShardInfo.RangeId
	s.Owner = resp.ShardInfo.Owner

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

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteImmediateTask_Single() {
	immediateTasks := s.AddRandomTasks(
		fakeImmediateTaskCategory,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			fakeTask := tasks.NewFakeTask(
				workflowKey,
				fakeImmediateTaskCategory,
				visibilityTimestamp,
			)
			fakeTask.SetTaskID(taskID)
			return fakeTask
		},
	)
	s.GetAndCompleteHistoryTask(fakeImmediateTaskCategory, immediateTasks[0])
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetRangeCompleteImmediateTasks_Multiple() {
	numTasks := 20
	immediateTasks := s.AddRandomTasks(
		fakeImmediateTaskCategory,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			fakeTask := tasks.NewFakeTask(
				workflowKey,
				fakeImmediateTaskCategory,
				visibilityTimestamp,
			)
			fakeTask.SetTaskID(taskID)
			return fakeTask
		},
	)

	immediateTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(immediateTasks)
	loadedTasks := s.PaginateTasks(
		fakeImmediateTaskCategory,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(immediateTasks)*2)+1,
	)
	s.Equal(immediateTasks, loadedTasks)

	err := s.ExecutionManager.RangeCompleteHistoryTasks(s.Ctx, &p.RangeCompleteHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        fakeImmediateTaskCategory,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(math.MaxInt64),
	})
	s.NoError(err)

	loadedTasks = s.PaginateTasks(
		fakeImmediateTaskCategory,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		1,
	)
	s.Empty(loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteScheduledTask_Single() {
	scheduledTasks := s.AddRandomTasks(
		fakeScheduledTaskCategory,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			fakeTask := tasks.NewFakeTask(
				workflowKey,
				fakeScheduledTaskCategory,
				visibilityTimestamp,
			)
			fakeTask.SetTaskID(taskID)
			return fakeTask
		},
	)
	s.GetAndCompleteHistoryTask(fakeScheduledTaskCategory, scheduledTasks[0])
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetRangeCompleteScheduledTasks_Multiple() {
	numTasks := 20
	scheduledTasks := s.AddRandomTasks(
		fakeScheduledTaskCategory,
		numTasks,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			fakeTask := tasks.NewFakeTask(
				workflowKey,
				fakeScheduledTaskCategory,
				visibilityTimestamp,
			)
			fakeTask.SetTaskID(taskID)
			return fakeTask
		},
	)

	scheduledTasks, inclusiveMinTaskKey, exclusiveMaxTaskKey := s.RandomPaginateRange(scheduledTasks)
	loadedTasks := s.PaginateTasks(
		fakeScheduledTaskCategory,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		rand.Intn(len(scheduledTasks)*2)+1,
	)
	s.Equal(scheduledTasks, loadedTasks)

	err := s.ExecutionManager.RangeCompleteHistoryTasks(s.Ctx, &p.RangeCompleteHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        fakeScheduledTaskCategory,
		InclusiveMinTaskKey: tasks.NewKey(time.Unix(0, 0), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(time.Unix(0, math.MaxInt64), 0),
	})
	s.NoError(err)

	loadedTasks = s.PaginateTasks(
		fakeScheduledTaskCategory,
		inclusiveMinTaskKey,
		exclusiveMaxTaskKey,
		1,
	)
	s.Empty(loadedTasks)
}

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteTransferTask_Single() {
	transferTasks := s.AddRandomTasks(
		tasks.CategoryTransfer,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.ActivityTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)
	s.GetAndCompleteHistoryTask(tasks.CategoryTransfer, transferTasks[0])
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

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteTimerTask_Single() {
	timerTasks := s.AddRandomTasks(
		tasks.CategoryTimer,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.UserTimerTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)
	s.GetAndCompleteHistoryTask(tasks.CategoryTimer, timerTasks[0])
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

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteReplicationTask_Single() {
	replicationTasks := s.AddRandomTasks(
		tasks.CategoryReplication,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.HistoryReplicationTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)
	s.GetAndCompleteHistoryTask(tasks.CategoryReplication, replicationTasks[0])
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

func (s *ExecutionMutableStateTaskSuite) TestAddGetCompleteVisibilityTask_Single() {
	visibilityTasks := s.AddRandomTasks(
		tasks.CategoryVisibility,
		1,
		func(workflowKey definition.WorkflowKey, taskID int64, visibilityTimestamp time.Time) tasks.Task {
			return &tasks.StartExecutionVisibilityTask{
				WorkflowKey:         workflowKey,
				TaskID:              taskID,
				VisibilityTimestamp: visibilityTimestamp,
			}
		},
	)
	s.GetAndCompleteHistoryTask(tasks.CategoryVisibility, visibilityTasks[0])
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

func (s *ExecutionMutableStateTaskSuite) TestIsReplicationDLQEmpty() {
	testShardID := int32(1)
	isEmpty, err := s.ExecutionManager.IsReplicationDLQEmpty(context.Background(), &p.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: p.GetHistoryTasksRequest{
			ShardID:             testShardID,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		},
		SourceClusterName: "test",
	})
	s.NoError(err)
	s.True(isEmpty)
	err = s.ExecutionManager.PutReplicationTaskToDLQ(context.Background(), &p.PutReplicationTaskToDLQRequest{
		ShardID:           testShardID,
		SourceClusterName: "test",
		TaskInfo:          &persistencespb.ReplicationTaskInfo{},
	})
	s.NoError(err)
	isEmpty, err = s.ExecutionManager.IsReplicationDLQEmpty(context.Background(), &p.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: p.GetHistoryTasksRequest{
			ShardID:             testShardID,
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(0),
		},
		SourceClusterName: "test",
	})
	s.NoError(err)
	s.False(isEmpty)
}

func (s *ExecutionMutableStateTaskSuite) TestGetTimerTasksOrdered() {
	now := time.Now().Truncate(p.ScheduledTaskMinPrecision)
	timerTasks := []tasks.Task{
		&tasks.UserTimerTask{
			WorkflowKey:         s.WorkflowKey,
			TaskID:              100,
			VisibilityTimestamp: now.Add(time.Nanosecond * 10),
		},
		&tasks.UserTimerTask{
			WorkflowKey:         s.WorkflowKey,
			TaskID:              50,
			VisibilityTimestamp: now.Add(time.Nanosecond * 20),
		},
	}

	err := s.ExecutionManager.AddHistoryTasks(s.Ctx, &p.AddHistoryTasksRequest{
		ShardID:     s.ShardID,
		RangeID:     s.RangeID,
		NamespaceID: s.WorkflowKey.NamespaceID,
		WorkflowID:  s.WorkflowKey.WorkflowID,
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTimer: timerTasks,
		},
	})
	s.NoError(err)

	// due to persistence layer precision loss,
	// two tasks can be returned in either order,
	// but must be ordered in terms of tasks.Key
	loadedTasks := s.PaginateTasks(
		tasks.CategoryTimer,
		tasks.NewKey(now, 0),
		tasks.NewKey(now.Add(time.Second), 0),
		10,
	)
	s.Len(loadedTasks, 2)
	s.True(loadedTasks[0].GetKey().CompareTo(loadedTasks[1].GetKey()) < 0)
}

func (s *ExecutionMutableStateTaskSuite) TestGetScheduledTasksOrdered() {
	now := time.Now().Truncate(p.ScheduledTaskMinPrecision)
	scheduledTasks := []tasks.Task{
		tasks.NewFakeTask(
			s.WorkflowKey,
			fakeScheduledTaskCategory,
			now.Add(time.Nanosecond*10),
		),
		tasks.NewFakeTask(
			s.WorkflowKey,
			fakeScheduledTaskCategory,
			now.Add(time.Nanosecond*20),
		),
	}
	scheduledTasks[0].SetTaskID(100)
	scheduledTasks[1].SetTaskID(50)

	err := s.ExecutionManager.AddHistoryTasks(s.Ctx, &p.AddHistoryTasksRequest{
		ShardID:     s.ShardID,
		RangeID:     s.RangeID,
		NamespaceID: s.WorkflowKey.NamespaceID,
		WorkflowID:  s.WorkflowKey.WorkflowID,
		Tasks: map[tasks.Category][]tasks.Task{
			fakeScheduledTaskCategory: scheduledTasks,
		},
	})
	s.NoError(err)

	// due to persistence layer precision loss,
	// two tasks can be returned in either order,
	// but must be ordered in terms of tasks.Key
	loadedTasks := s.PaginateTasks(
		fakeScheduledTaskCategory,
		tasks.NewKey(now, 0),
		tasks.NewKey(now.Add(time.Second), 0),
		10,
	)
	s.Len(loadedTasks, 2)
	s.True(loadedTasks[0].GetKey().CompareTo(loadedTasks[1].GetKey()) < 0)

	err = s.ExecutionManager.RangeCompleteHistoryTasks(s.Ctx, &p.RangeCompleteHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        fakeScheduledTaskCategory,
		InclusiveMinTaskKey: tasks.NewKey(now, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(now.Add(time.Second), 0),
	})
	s.NoError(err)

	response, err := s.ExecutionManager.GetHistoryTasks(s.Ctx, &p.GetHistoryTasksRequest{
		ShardID:             s.ShardID,
		TaskCategory:        fakeScheduledTaskCategory,
		InclusiveMinTaskKey: tasks.NewKey(now, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(now.Add(time.Second), 0),
		BatchSize:           10,
	})
	s.NoError(err)
	s.Empty(response.Tasks)
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
		now = now.Truncate(p.ScheduledTaskMinPrecision)
		randomTasks = append(randomTasks, newTaskFn(s.WorkflowKey, currentTaskID, now))
		currentTaskID += rand.Int63n(100) + 1
		now = now.Add(time.Duration(rand.Int63n(1000_000_000)) + time.Millisecond)
	}

	err := s.ExecutionManager.AddHistoryTasks(s.Ctx, &p.AddHistoryTasksRequest{
		ShardID:     s.ShardID,
		RangeID:     s.RangeID,
		NamespaceID: s.WorkflowKey.NamespaceID,
		WorkflowID:  s.WorkflowKey.WorkflowID,
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

func (s *ExecutionMutableStateTaskSuite) GetAndCompleteHistoryTask(
	category tasks.Category,
	task tasks.Task,
) {
	key := task.GetKey()
	var minKey, maxKey tasks.Key
	if category.Type() == tasks.CategoryTypeImmediate {
		minKey = key
		maxKey = minKey.Next()
	} else {
		minKey = tasks.NewKey(key.FireTime, 0)
		maxKey = tasks.NewKey(key.FireTime.Add(persistence.ScheduledTaskMinPrecision), 0)
	}

	historyTasks := s.PaginateTasks(category, minKey, maxKey, 1)
	s.Len(historyTasks, 1)
	s.Equal(task, historyTasks[0])

	err := s.ExecutionManager.CompleteHistoryTask(s.Ctx, &p.CompleteHistoryTaskRequest{
		ShardID:      s.ShardID,
		TaskCategory: category,
		TaskKey:      key,
	})
	s.NoError(err)

	historyTasks = s.PaginateTasks(category, minKey, maxKey, 1)
	s.Empty(historyTasks)
}

func newTestSerializer(
	serializer serialization.Serializer,
) serialization.Serializer {
	return &testSerializer{
		Serializer: serializer,
	}
}

func (s *testSerializer) SerializeTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	if fakeTask, ok := task.(*tasks.FakeTask); ok {
		data, err := proto.Marshal(&persistencespb.TransferTaskInfo{
			NamespaceId:    fakeTask.WorkflowKey.NamespaceID,
			WorkflowId:     fakeTask.WorkflowKey.WorkflowID,
			RunId:          fakeTask.WorkflowKey.RunID,
			TaskType:       fakeTask.GetType(),
			Version:        fakeTask.Version,
			TaskId:         fakeTask.TaskID,
			VisibilityTime: timestamppb.New(fakeTask.VisibilityTimestamp),
		})
		if err != nil {
			return nil, err
		}
		return &commonpb.DataBlob{
			Data:         data,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}, nil
	}

	return s.Serializer.SerializeTask(task)
}

func (s *testSerializer) DeserializeTask(
	category tasks.Category,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	categoryID := category.ID()
	if categoryID != fakeImmediateTaskCategory.ID() &&
		categoryID != fakeScheduledTaskCategory.ID() {
		return s.Serializer.DeserializeTask(category, blob)
	}

	taskInfo := &persistencespb.TransferTaskInfo{}
	if err := proto.Unmarshal(blob.Data, taskInfo); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	fakeTask := tasks.NewFakeTask(
		definition.NewWorkflowKey(
			taskInfo.NamespaceId,
			taskInfo.WorkflowId,
			taskInfo.RunId,
		),
		category,
		taskInfo.VisibilityTime.AsTime(),
	)
	fakeTask.SetTaskID(taskInfo.TaskId)

	return fakeTask, nil
}
