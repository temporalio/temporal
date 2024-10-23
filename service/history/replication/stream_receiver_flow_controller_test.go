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

package replication

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	flowControlTestSuite struct {
		suite.Suite
		ctrl                *gomock.Controller
		controller          *streamReceiverFlowControllerImpl
		config              *configs.Config
		maxOutStandingTasks int
	}
)

func (f *flowControlTestSuite) SetupTest() {
	lowPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 5}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 150}
	}

	signals := map[enums.TaskPriority]FlowControlSignalProvider{
		enums.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enums.TASK_PRIORITY_HIGH: highPrioritySignal,
	}

	f.config = tests.NewDynamicConfig()
	f.config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	f.controller = NewReceiverFlowControl(signals, f.config)
	f.maxOutStandingTasks = f.config.ReplicationReceiverMaxOutstandingTaskCount()
}

func TestFlowControlTestSuite(t *testing.T) {
	suite.Run(t, new(flowControlTestSuite))
}

func (f *flowControlTestSuite) TestLowPriorityWithinLimit() {
	actual := f.controller.GetFlowControlInfo(enums.TASK_PRIORITY_LOW)
	expected := enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestHighPriorityExceedsLimit() {
	actual := f.controller.GetFlowControlInfo(enums.TASK_PRIORITY_HIGH)
	expected := enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestUnknownPriority() {
	unknownPriority := enums.TaskPriority(999) // Assuming 999 is an unknown priority
	actual := f.controller.GetFlowControlInfo(unknownPriority)
	expected := enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestBoundaryCondition() {
	boundarySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: f.maxOutStandingTasks}
	}

	signals := map[enums.TaskPriority]FlowControlSignalProvider{
		enums.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual := f.controller.GetFlowControlInfo(enums.TASK_PRIORITY_LOW)
	expected := enums.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	boundarySignal = func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: f.maxOutStandingTasks + 1}
	}

	signals = map[enums.TaskPriority]FlowControlSignalProvider{
		enums.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enums.TASK_PRIORITY_LOW)
	expected = enums.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}
