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

package listtasks

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	apiSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockExecutionManager *persistence.MockExecutionManager

		taskCategoryRegistry tasks.TaskCategoryRegistry
	}
)

func TestAPISuite(t *testing.T) {
	s := new(apiSuite)
	suite.Run(t, s)
}

func (s *apiSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)

	s.taskCategoryRegistry = tasks.NewDefaultTaskCategoryRegistry()
}

func (s *apiSuite) TestInvalidTaskCategory() {
	invalidCategoryID := int32(-1)
	request := &historyservice.ListTasksRequest{
		Request: &adminservice.ListHistoryTasksRequest{
			ShardId:  1,
			Category: invalidCategoryID,
			TaskRange: &history.TaskRange{
				InclusiveMinTaskKey: &history.TaskKey{
					TaskId: 10,
				},
				ExclusiveMaxTaskKey: &history.TaskKey{
					TaskId: 20,
				},
			},
			BatchSize:     100,
			NextPageToken: nil,
		},
	}

	_, err := Invoke(
		context.Background(),
		s.taskCategoryRegistry,
		s.mockExecutionManager,
		request,
	)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.ErrorContains(err, strconv.Itoa(int(invalidCategoryID)))
}

func (s *apiSuite) TestInvalidTaskRange() {
	invalidTaskID := int64(-1)
	request := &historyservice.ListTasksRequest{
		Request: &adminservice.ListHistoryTasksRequest{
			ShardId:  1,
			Category: tasks.CategoryIDTransfer,
			TaskRange: &history.TaskRange{
				InclusiveMinTaskKey: &history.TaskKey{
					TaskId:   -1,
					FireTime: timestamppb.New(time.Unix(0, 0)),
				},
				ExclusiveMaxTaskKey: &history.TaskKey{
					TaskId:   20,
					FireTime: timestamppb.New(time.Unix(0, 0)),
				},
			},
			BatchSize:     100,
			NextPageToken: nil,
		},
	}

	_, err := Invoke(
		context.Background(),
		s.taskCategoryRegistry,
		s.mockExecutionManager,
		request,
	)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.ErrorContains(err, strconv.Itoa(int(invalidTaskID)))
}

func (s *apiSuite) TestGetHistoryTasks() {
	batchSize := 2
	reqNextPageToken := []byte("req-next-page-token")
	minTaskID := int64(10)
	maxTaskID := minTaskID + 2*int64(batchSize)
	taskVersion := int64(123)

	request := &historyservice.ListTasksRequest{
		Request: &adminservice.ListHistoryTasksRequest{
			ShardId:  1,
			Category: tasks.CategoryIDTransfer,
			TaskRange: &history.TaskRange{
				InclusiveMinTaskKey: &history.TaskKey{
					TaskId:   minTaskID,
					FireTime: timestamppb.New(time.Unix(0, 0)),
				},
				ExclusiveMaxTaskKey: &history.TaskKey{
					TaskId:   maxTaskID,
					FireTime: timestamppb.New(time.Unix(0, 0)),
				},
			},
			BatchSize:     int32(batchSize),
			NextPageToken: reqNextPageToken,
		},
	}

	respNextPageToken := []byte("resp-next-page-token")
	fakeTasks := make([]tasks.Task, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		fakeTask := tasks.NewFakeTask(
			tests.WorkflowKey,
			tasks.CategoryTransfer,
			time.Now(),
		).(*tasks.FakeTask)
		fakeTask.SetTaskID(minTaskID + int64(i))
		fakeTask.SetVersion(taskVersion)
		fakeTasks = append(fakeTasks, fakeTask)
	}

	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             1,
		TaskCategory:        tasks.CategoryTransfer,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID),
		BatchSize:           batchSize,
		NextPageToken:       reqNextPageToken,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         fakeTasks,
		NextPageToken: respNextPageToken,
	}, nil)

	resp, err := Invoke(
		context.Background(),
		s.taskCategoryRegistry,
		s.mockExecutionManager,
		request,
	)
	s.NoError(err)

	expectedAdminTasks := make([]*adminservice.Task, 0, batchSize)
	for i := range fakeTasks {
		adminTask := &adminservice.Task{
			NamespaceId: tests.WorkflowKey.NamespaceID,
			WorkflowId:  tests.WorkflowKey.WorkflowID,
			RunId:       tests.WorkflowKey.RunID,
			TaskId:      int64(minTaskID + int64(i)),
			TaskType:    enumsspb.TASK_TYPE_UNSPECIFIED,
			FireTime:    timestamppb.New(time.Unix(0, 0)),
			Version:     taskVersion,
		}
		expectedAdminTasks = append(expectedAdminTasks, adminTask)
	}

	protoassert.ProtoEqual(s.T(), &historyservice.ListTasksResponse{
		Response: &adminservice.ListHistoryTasksResponse{
			Tasks:         expectedAdminTasks,
			NextPageToken: respNextPageToken,
		},
	}, resp)
}
