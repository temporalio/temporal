package replication

import (
	"testing"
	"time"

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
		return &FlowControlSignal{
			taskTrackingCount:  5,
			lastSlowSubmission: time.Time{}, // zero time means no slow submission
		}
	}

	highPrioritySignal := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  150,
			lastSlowSubmission: time.Time{}, // zero time means no slow submission
		}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW:  lowPrioritySignal,
		enumsspb.TASK_PRIORITY_HIGH: highPrioritySignal,
	}

	f.config = tests.NewDynamicConfig()
	f.config.ReplicationReceiverMaxOutstandingTaskCount = func() int {
		return 50
	}
	f.config.ReplicationReceiverSlowSubmissionLatencyThreshold = func() time.Duration {
		return 1 * time.Second
	}
	f.config.ReplicationReceiverSlowSubmissionWindow = func() time.Duration {
		return 5 * time.Second
	}
	f.config.EnableReplicationReceiverSlowSubmissionFlowControl = func() bool {
		return false
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
		return &FlowControlSignal{
			taskTrackingCount:  f.maxOutStandingTasks,
			lastSlowSubmission: time.Time{},
		}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	boundarySignal = func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  f.maxOutStandingTasks + 1,
			lastSlowSubmission: time.Time{},
		}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: boundarySignal,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}

func (f *flowControlTestSuite) TestSubmitLatency() {
	f.config.EnableReplicationReceiverSlowSubmissionFlowControl = func() bool {
		return true
	}
	slowSubmissionWindow := f.config.ReplicationReceiverSlowSubmissionWindow()
	now := time.Now()

	// Test that slow submission timestamp within window triggers pause
	signalWithSlowSubmit := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  10,
			lastSlowSubmission: now.Add(-slowSubmissionWindow / 2), // slow submission detected recently
		}
	}

	signals := map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalWithSlowSubmit,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual := f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected := enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)

	// Test that no slow submission (zero time) doesn't trigger pause
	signalWithNoSlowSubmit := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  10,
			lastSlowSubmission: time.Time{}, // no slow submission detected
		}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalWithNoSlowSubmit,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	// Test that slow submission outside window doesn't trigger pause
	signalWithStaleSlowSubmit := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  10,
			lastSlowSubmission: now.Add(-slowSubmissionWindow - time.Second), // slow submission detected outside window
		}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalWithStaleSlowSubmit,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	// Test that task tracking count and submit latency are checked independently
	// Both within limits - should resume
	signalBothWithinLimits := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  30,
			lastSlowSubmission: time.Time{},
		}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalBothWithinLimits,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
	f.Equal(expected, actual)

	// Task tracking count exceeds limit - should pause (even though no slow submission)
	signalTaskTrackingExceeds := func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  f.maxOutStandingTasks + 1,
			lastSlowSubmission: time.Time{},
		}
	}

	signals = map[enumsspb.TaskPriority]FlowControlSignalProvider{
		enumsspb.TASK_PRIORITY_LOW: signalTaskTrackingExceeds,
	}

	f.controller = NewReceiverFlowControl(signals, f.config)

	actual = f.controller.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
	expected = enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	f.Equal(expected, actual)
}
