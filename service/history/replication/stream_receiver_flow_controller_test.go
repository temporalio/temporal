package replication

import (
	"testing"

	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
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
		return &FlowControlSignal{taskTrackingCount: 5, schedulerQueueCount: 0}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 150, schedulerQueueCount: 0}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enumsspb.TASK_PRIORITY_HIGH: highPrioritySignal,
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
	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestHighPriorityExceedsLimit() {
	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestUnknownPriority() {
	unknownPriority := enumsspb.TaskPriority(999) // Assuming 999 is an unknown priority
	actual := f.controller.GetFlowControlInfo(unknownPriority)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestBoundaryCondition() {
	boundarySignal := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: f.maxOutStandingTasks, schedulerQueueCount: 0}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	boundarySignal = func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: f.maxOutStandingTasks + 1, schedulerQueueCount: 0}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestSchedulerQueueCount() {
	// Test that scheduler queue count is included in flow control decision
	signalWithSchedulerQueue := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 30, schedulerQueueCount: 25}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalWithSchedulerQueue,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	// Total: 30 + 25 = 55, which exceeds limit of 50
	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)

	// Test that scheduler queue count alone can trigger pause
	signalWithOnlySchedulerQueue := func() *FlowControlSignal {
		return &FlowControlSignal{taskTrackingCount: 0, schedulerQueueCount: 51}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalWithOnlySchedulerQueue,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}
