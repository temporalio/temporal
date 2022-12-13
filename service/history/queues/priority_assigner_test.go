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
	"go.temporal.io/server/common/tasks"
)

type (
	priorityAssignerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

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

	s.priorityAssigner = NewPriorityAssigner().(*priorityAssignerImpl)
}

func (s *priorityAssignerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *priorityAssignerSuite) TestAssign_SelectedTaskTypes() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT).Times(1)

	s.Equal(tasks.PriorityLow, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_UnknownTaskTypes() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TaskType(1234)).Times(1)

	s.Equal(tasks.PriorityLow, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_HighPriorityTaskTypes() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER).Times(1)

	s.Equal(tasks.PriorityHigh, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_LowPriorityTaskTypes() {
	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		enumsspb.TASK_TYPE_UNSPECIFIED,
	} {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		s.Equal(tasks.PriorityLow, s.priorityAssigner.Assign(mockExecutable))
	}
}
