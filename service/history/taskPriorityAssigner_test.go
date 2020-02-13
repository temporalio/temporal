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

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	taskPriorityAssignerSuite struct {
		*require.Assertions
		suite.Suite

		controller      *gomock.Controller
		mockDomainCache *cache.MockDomainCache

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
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)

	s.testTaskProcessRPS = 10
	dc := dynamicconfig.NewNopCollection()
	config := NewDynamicConfigForTest()
	config.TaskProcessRPS = dc.GetIntPropertyFilteredByDomain(dynamicconfig.TaskProcessRPS, s.testTaskProcessRPS)

	s.priorityAssigner = newTaskPriorityAssigner(
		cluster.TestCurrentClusterName,
		s.mockDomainCache,
		log.NewNoop(),
		metrics.NewClient(tally.NoopScope, metrics.History),
		config,
	)
}

func (s *taskPriorityAssignerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskPriorityAssignerSuite) TestGetDomainInfo_Success_Active() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil)

	domainName, isActive, err := s.priorityAssigner.getDomainInfo(testDomainID)
	s.NoError(err)
	s.Equal(testDomainName, domainName)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetDomainInfo_Success_Passive() {
	testGlobalDomainEntry.GetReplicationConfig().ActiveClusterName = cluster.TestAlternativeClusterName
	defer func() {
		testGlobalDomainEntry.GetReplicationConfig().ActiveClusterName = cluster.TestCurrentClusterName
	}()
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil)

	domainName, isActive, err := s.priorityAssigner.getDomainInfo(testDomainID)
	s.NoError(err)
	s.Equal(testDomainName, domainName)
	s.False(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetDomainInfo_Success_Local() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testLocalDomainEntry, nil)

	domainName, isActive, err := s.priorityAssigner.getDomainInfo(testDomainID)
	s.NoError(err)
	s.Equal(testDomainName, domainName)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetDomainInfo_Fail_DomainNotExist() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(
		nil,
		&workflow.EntityNotExistsError{Message: "domain not exist"},
	)

	domainName, isActive, err := s.priorityAssigner.getDomainInfo(testDomainID)
	s.NoError(err)
	s.Empty(domainName)
	s.True(isActive)
}

func (s *taskPriorityAssignerSuite) TestGetDomainInfo_Fail_UnknownError() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(
		nil,
		errors.New("some random error"),
	)

	domainName, isActive, err := s.priorityAssigner.getDomainInfo(testDomainID)
	s.Error(err)
	s.Empty(domainName)
	s.False(isActive)
}

func (s *taskPriorityAssignerSuite) TestAssign_ReplicationTask() {
	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(replicationQueueType).Times(1)
	mockTask.EXPECT().SetPriority(getTaskPriority(taskLowPriorityClass, taskDefaultPrioritySubclass)).Times(1)

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_StandbyTask() {
	testGlobalDomainEntry.GetReplicationConfig().ActiveClusterName = cluster.TestAlternativeClusterName
	defer func() {
		testGlobalDomainEntry.GetReplicationConfig().ActiveClusterName = cluster.TestCurrentClusterName
	}()
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(transferQueueType).Times(1)
	mockTask.EXPECT().GetDomainID().Return(testDomainID).Times(1)
	mockTask.EXPECT().SetPriority(getTaskPriority(taskLowPriorityClass, taskDefaultPrioritySubclass)).Times(1)

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_TransferTask() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(transferQueueType).AnyTimes()
	mockTask.EXPECT().GetDomainID().Return(testDomainID).Times(1)
	mockTask.EXPECT().SetPriority(getTaskPriority(taskHighPriorityClass, taskDefaultPrioritySubclass)).Times(1)

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_TimerTask() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil)

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetQueueType().Return(timerQueueType).AnyTimes()
	mockTask.EXPECT().GetDomainID().Return(testDomainID).Times(1)
	mockTask.EXPECT().SetPriority(getTaskPriority(taskHighPriorityClass, taskDefaultPrioritySubclass)).Times(1)

	err := s.priorityAssigner.Assign(mockTask)
	s.NoError(err)
}

func (s *taskPriorityAssignerSuite) TestAssign_ThrottledTask() {
	s.mockDomainCache.EXPECT().GetDomainByID(testDomainID).Return(testGlobalDomainEntry, nil).AnyTimes()

	for i := 0; i != s.testTaskProcessRPS*2; i++ {
		mockTask := NewMockqueueTask(s.controller)
		mockTask.EXPECT().GetQueueType().Return(timerQueueType).AnyTimes()
		mockTask.EXPECT().GetDomainID().Return(testDomainID).Times(1)
		if i < s.testTaskProcessRPS {
			mockTask.EXPECT().SetPriority(getTaskPriority(taskHighPriorityClass, taskDefaultPrioritySubclass)).Times(1)
		} else {
			mockTask.EXPECT().SetPriority(getTaskPriority(taskDefaultPriorityClass, taskDefaultPrioritySubclass)).Times(1)
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
			class:            taskHighPriorityClass,
			subClass:         taskDefaultPrioritySubclass,
			expectedPriority: 1,
		},
		{
			class:            taskDefaultPriorityClass,
			subClass:         taskLowPrioritySubclass,
			expectedPriority: 10,
		},
		{
			class:            taskLowPriorityClass,
			subClass:         taskHighPrioritySubclass,
			expectedPriority: 16,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedPriority, getTaskPriority(tc.class, tc.subClass))
	}
}
