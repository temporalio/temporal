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

package history

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/service/history/configs"
)

type (
	taskPriorityAssignerSuite struct {
		*require.Assertions
		suite.Suite

		controller         *gomock.Controller
		mockNamespaceCache *cache.MockNamespaceCache

		priorityAssigner   *taskPriorityAssignerImpl
		testTaskProcessRPS int
	}
)

func TestTaskPriorityAssignerSuite(t *testing.T) {
	s := new(taskPriorityAssignerSuite)
	suite.Run(t, s)
}

func (s *taskPriorityAssignerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)

	s.testTaskProcessRPS = 10
	dc := dynamicconfig.NewNopCollection()
	config := NewDynamicConfigForTest()
	config.TaskProcessRPS = dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskProcessRPS, s.testTaskProcessRPS)

	s.priorityAssigner = newTaskPriorityAssigner(
		cluster.TestCurrentClusterName,
		s.mockNamespaceCache,
		log.NewNoop(),
		metrics.NewClient(tally.NoopScope, metrics.History),
		config,
	)
}

func (s *taskPriorityAssignerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskPriorityAssignerSuite) TestGetNamespaceInfo_Success_Active() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil)

	namespace, isActive, err := s.priorityAssigner.getNamespaceInfo(testNamespaceID)
	s.NoError(err)
	s.Equal(testNamespace, namespace)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetNamespaceInfo_Success_Passive() {
	testGlobalNamespaceEntry.GetReplicationConfig().ActiveClusterName = cluster.TestAlternativeClusterName
	defer func() {
		testGlobalNamespaceEntry.GetReplicationConfig().ActiveClusterName = cluster.TestCurrentClusterName
	}()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil)

	namespace, isActive, err := s.priorityAssigner.getNamespaceInfo(testNamespaceID)
	s.NoError(err)
	s.Equal(testNamespace, namespace)
	s.False(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetNamespaceInfo_Success_Local() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testLocalNamespaceEntry, nil)

	namespace, isActive, err := s.priorityAssigner.getNamespaceInfo(testNamespaceID)
	s.NoError(err)
	s.Equal(testNamespace, namespace)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetNamespaceInfo_Fail_NamespaceNotExist() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(
		nil,
		serviceerror.NewNotFound("namespace not exist"),
	)

	namespace, isActive, err := s.priorityAssigner.getNamespaceInfo(testNamespaceID)
	s.NoError(err)
	s.Empty(namespace)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetNamespaceInfo_Fail_UnknownError() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(
		nil,
		errors.New("some random error"),
	)

	namespace, isActive, err := s.priorityAssigner.getNamespaceInfo(testNamespaceID)
	s.Error(err)
	s.Empty(namespace)
	s.False(isActive)
}

func (s *taskPriorityAssignerSuite) TestAssign_ReplicationTask() {
	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(replicationQueueType)
	mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskLowPriorityClass, configs.TaskDefaultPrioritySubclass))

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_StandbyTask() {
	testGlobalNamespaceEntry.GetReplicationConfig().ActiveClusterName = cluster.TestAlternativeClusterName
	defer func() {
		testGlobalNamespaceEntry.GetReplicationConfig().ActiveClusterName = cluster.TestCurrentClusterName
	}()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(transferQueueType)
	mockTask.EXPECT().GetNamespaceID().Return(testNamespaceID)
	mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskLowPriorityClass, configs.TaskDefaultPrioritySubclass))

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_TransferTask() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(transferQueueType).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(testNamespaceID)
	mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskHighPriorityClass, configs.TaskDefaultPrioritySubclass))

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_TimerTask() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(timerQueueType).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(testNamespaceID)
	mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskHighPriorityClass, configs.TaskDefaultPrioritySubclass))

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_ThrottledTask() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(testNamespaceID).Return(testGlobalNamespaceEntry, nil).AnyTimes()

	for i := 0; i != s.testTaskProcessRPS*2; i++ {
		mockTask := NewMockqueueTask(s.controller)
		mockTask.EXPECT().GetQueueType().Return(timerQueueType).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(testNamespaceID)
		if i < s.testTaskProcessRPS {
			mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskHighPriorityClass, configs.TaskDefaultPrioritySubclass))
		} else {
			mockTask.EXPECT().SetPriority(configs.GetTaskPriority(configs.TaskDefaultPriorityClass, configs.TaskDefaultPrioritySubclass))
		}

		err := s.priorityAssigner.Assign(mockTask)
		s.NoError(err)
	}
}

func (s *taskPriorityAssignerSuite) TestGetTaskPriority() {
	testCases := []struct {
		class            int
		subClass         int
		expectedPriority int
	}{
		{
			class:            configs.TaskHighPriorityClass,
			subClass:         configs.TaskDefaultPrioritySubclass,
			expectedPriority: 1,
		},
		{
			class:            configs.TaskDefaultPriorityClass,
			subClass:         configs.TaskLowPrioritySubclass,
			expectedPriority: 10,
		},
		{
			class:            configs.TaskLowPriorityClass,
			subClass:         configs.TaskHighPrioritySubclass,
			expectedPriority: 16,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedPriority, configs.GetTaskPriority(tc.class, tc.subClass))
	}
}
