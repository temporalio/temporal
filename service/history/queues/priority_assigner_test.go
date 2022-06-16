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

package queues

import (
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	priorityAssignerSuite struct {
		*require.Assertions
		suite.Suite

		controller            *gomock.Controller
		mockNamespaceRegistry *namespace.MockRegistry

		priorityAssigner *priorityAssignerImpl
	}
)

func TestPriorityAssignerSuite(t *testing.T) {
	s := new(priorityAssignerSuite)
	suite.Run(t, s)
}

func (s *priorityAssignerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)

	s.priorityAssigner = NewPriorityAssigner(
		cluster.TestCurrentClusterName,
		s.mockNamespaceRegistry,
		PriorityAssignerOptions{
			HighPriorityRPS:       dynamicconfig.GetIntPropertyFilteredByNamespace(3),
			CriticalRetryAttempts: dynamicconfig.GetIntPropertyFn(100),
		},
		metrics.NoopMetricsHandler,
	).(*priorityAssignerImpl)
}

func (s *priorityAssignerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *priorityAssignerSuite) TestAssign_CriticalAttempts() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().Attempt().Return(1000).AnyTimes()
	mockExecutable.EXPECT().SetPriority(tasks.PriorityLow)

	err := s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)
}

func (s *priorityAssignerSuite) TestAssign_StandbyNamespaceStandbyQueue() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalStandbyNamespaceEntry, nil)

	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().Attempt().Return(10).AnyTimes()
	mockExecutable.EXPECT().GetNamespaceID().Return(tests.NamespaceID.String()).AnyTimes()
	mockExecutable.EXPECT().QueueType().Return(QueueTypeStandbyTransfer).AnyTimes()
	mockExecutable.EXPECT().SetPriority(tasks.PriorityLow)

	err := s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)
}

func (s *priorityAssignerSuite) TestAssign_NoopExecuable() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalStandbyNamespaceEntry, nil)

	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().Attempt().Return(10).AnyTimes()
	mockExecutable.EXPECT().GetNamespaceID().Return(tests.NamespaceID.String()).AnyTimes()
	mockExecutable.EXPECT().QueueType().Return(QueueTypeActiveTransfer).AnyTimes()
	mockExecutable.EXPECT().SetPriority(tasks.PriorityHigh)

	err := s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)
}

func (s *priorityAssignerSuite) TestAssign_SelectedTaskTypes() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil)

	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().Attempt().Return(10).AnyTimes()
	mockExecutable.EXPECT().GetNamespaceID().Return(tests.NamespaceID.String()).AnyTimes()
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT).AnyTimes()

	mockExecutable.EXPECT().QueueType().Return(QueueTypeActiveTransfer).AnyTimes()
	mockExecutable.EXPECT().SetPriority(tasks.PriorityMedium)

	err := s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)
}

func (s *priorityAssignerSuite) TestAssign_Throttled() {
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	rps := s.priorityAssigner.options.HighPriorityRPS("")
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().Attempt().Return(10).AnyTimes()
	mockExecutable.EXPECT().GetNamespaceID().Return(tests.NamespaceID.String()).AnyTimes()
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_UNSPECIFIED).AnyTimes()
	mockExecutable.EXPECT().QueueType().Return(QueueTypeActiveTransfer).AnyTimes()

	mockExecutable.EXPECT().SetPriority(tasks.PriorityHigh).Times(1)
	err := s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)

	for i := 0; i != rps*3; i++ {
		mockExecutable.EXPECT().SetPriority(gomock.Any()).Times(1)
		err := s.priorityAssigner.Assign(mockExecutable)
		s.NoError(err)
	}

	mockExecutable.EXPECT().SetPriority(tasks.PriorityMedium).Times(1)
	err = s.priorityAssigner.Assign(mockExecutable)
	s.NoError(err)
}
