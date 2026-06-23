package api

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	nonTerminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
		{CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES},
		{CommandType: enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES},
	}

	terminalCommands = []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION},
		{CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION},
	}
)

type commandAttrValidatorSuite struct {
	suite.Suite
	*require.Assertions

	controller            *gomock.Controller
	mockNamespaceRegistry *namespace.MockRegistry
	mockVisibilityManager *manager.MockVisibilityManager
	validator             *CommandAttrValidator
	mockClusterMetadata   *cluster.MockMetadata
}

func TestCommandAttrValidatorSuite(t *testing.T) {
	suite.Run(t, new(commandAttrValidatorSuite))
}

func (s *commandAttrValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockVisibilityManager = manager.NewMockVisibilityManager(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)

	cfg := tests.NewDynamicConfig()
	s.validator = NewCommandAttrValidator(
		s.mockNamespaceRegistry,
		cfg,
		searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			s.mockVisibilityManager,
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(256),
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
			false,
		),
	)
}

func (s *commandAttrValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_NoTerminalCommand() {
	err := s.validator.ValidateCommandSequence(nonTerminalCommands)
	s.NoError(err)
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_ValidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.ValidateCommandSequence(append(nonTerminalCommands, terminalCommand))
		s.NoError(err)
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_InvalidTerminalCommand() {
	for _, terminalCommand := range terminalCommands {
		err := s.validator.ValidateCommandSequence(append(
			[]*commandpb.Command{terminalCommand},
			nonTerminalCommands[int(rand.Int31n(int32(len(nonTerminalCommands))))],
		))
		s.Error(err)
		s.IsType(&serviceerror.InvalidArgument{}, err)
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_Truncation() {
	// Build a command list that is long enough to exceed the truncation limit.
	// Each "RECORD_MARKER" entry is ~13 chars plus ", " (2 chars) = ~15 chars.
	// 2048 / 15 ≈ 137 commands. Use 200 to be well over the limit.
	var longCommands []*commandpb.Command
	for i := 0; i < 200; i++ {
		longCommands = append(longCommands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		})
	}
	// Prepend a terminal command so the validator hits the "must be last"
	// error on the second element (the first RECORD_MARKER after COMPLETE).
	longCommands = append(
		[]*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION}},
		longCommands...,
	)

	err := s.validator.ValidateCommandSequence(longCommands)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	msg := err.Error()
	s.LessOrEqual(len(msg), maxCommandSequenceErrorLength+100,
		"error message should be bounded even with a huge command list")
	s.Contains(msg, "...", "truncated command list should contain ellipsis")
	s.Contains(msg, "COMPLETE_WORKFLOW_EXECUTION", "first command type should still be visible")
}

func (s *commandAttrValidatorSuite) TestTruncatedCommandTypes_NoTruncation() {
	shortCommands := []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
		{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION},
	}
	result := s.validator.truncatedCommandTypes(shortCommands)
	s.Equal("SCHEDULE_ACTIVITY_TASK, RECORD_MARKER, COMPLETE_WORKFLOW_EXECUTION", result)
}

func (s *commandAttrValidatorSuite) TestTruncatedCommandTypes_Truncation() {
	var longCommands []*commandpb.Command
	for i := 0; i < 200; i++ {
		longCommands = append(longCommands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		})
	}
	result := s.validator.truncatedCommandTypes(longCommands)
	s.LessOrEqual(len(result), maxCommandSequenceErrorLength,
		"truncated result length %d should not exceed %d", len(result), maxCommandSequenceErrorLength)
	s.Contains(result, "...", "truncated result should contain ellipsis")
	s.NotContains(result, "RECORD_MARKER, RECORD_MARKER, RECORD_MARKER, RECORD_MARKER, RECORD_MARKER",
		"truncation should cut at a comma boundary, not mid-token")
}

func (s *commandAttrValidatorSuite) TestValidateStartChildExecutionAttributes_InternalTaskQueue() {
	testCases := []struct {
		name            string
		parentTaskQueue string
		childTaskQueue  string
		expectError     bool
	}{
		{
			name:            "internal parent task queue with non-internal child task queue is allowed",
			parentTaskQueue: primitives.PerNSWorkerTaskQueue,
			childTaskQueue:  "user-child-task-queue",
			expectError:     false,
		},
		{
			name:            "internal parent task queue with empty child task queue is allowed",
			parentTaskQueue: primitives.PerNSWorkerTaskQueue,
			childTaskQueue:  "",
			expectError:     false,
		},
		{
			name:            "internal parent task queue with internal child task queue is allowed",
			parentTaskQueue: primitives.PerNSWorkerTaskQueue,
			childTaskQueue:  primitives.PerNSWorkerTaskQueue,
			expectError:     false,
		},
		{
			name:            "non-internal parent task queue with internal child task queue is not allowed",
			parentTaskQueue: "user-parent-task-queue",
			childTaskQueue:  primitives.PerNSWorkerTaskQueue,
			expectError:     true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			err := s.validator.validateStartChildExecutionAttributes(
				&commandpb.StartChildWorkflowExecutionCommandAttributes{TaskQueue: &taskqueuepb.TaskQueue{Name: tc.childTaskQueue}},
				tc.parentTaskQueue,
			)
			if tc.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *commandAttrValidatorSuite) TestValidateActivityAttributes() {
	validTaskQueue := &taskqueuepb.TaskQueue{Name: "test-taskqueue"}
	validTimeout := durationpb.New(time.Hour)

	activityType := "test-activity-type"
	activityID := "test-activity-id"
	validActivityAttributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             activityID,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              validTaskQueue,
		Input:                  nil,
		ScheduleToCloseTimeout: validTimeout,
		ScheduleToStartTimeout: validTimeout,
		StartToCloseTimeout:    validTimeout,
		HeartbeatTimeout:       validTimeout,
	}

	// Test valid attributes
	s.NoError(s.validator.validateActivityAttributes("test-namespace", validActivityAttributes))

	// Test invalid activity ID length
	invalidActivityID := make([]byte, s.validator.maxIDLengthLimit+1)
	for i := range invalidActivityID {
		invalidActivityID[i] = 'a'
	}
	invalidActivityAttributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		ActivityId:             string(invalidActivityID),
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              validTaskQueue,
		ScheduleToCloseTimeout: validTimeout,
	}
	s.Error(s.validator.validateActivityAttributes("test-namespace", invalidActivityAttributes))
}

func (s *commandAttrValidatorSuite) TestValidateTimerAttributes() {
	validTimerID := "test-timer-id"
	validTimeout := durationpb.New(time.Hour)

	// Test valid timer attributes
	s.NoError(s.validator.validateTimerAttributes("test-namespace", validTimerID, validTimeout.AsDuration()))

	// Test invalid timer ID length
	invalidTimerID := make([]byte, s.validator.maxIDLengthLimit+1)
	for i := range invalidTimerID {
		invalidTimerID[i] = 'a'
	}
	s.Error(s.validator.validateTimerAttributes("test-namespace", string(invalidTimerID), validTimeout.AsDuration()))
}

func (s *commandAttrValidatorSuite) TestValidateMarkerAttributes() {
	validMarkerName := "test-marker-name"
	validDetails := map[string]*commonpb.Payloads{
		"test-key": payloads.EncodeString("test-value"),
	}

	// Test valid marker attributes
	s.NoError(s.validator.validateMarkerAttributes("test-namespace", validMarkerName, validDetails))

	// Test invalid marker name length
	invalidMarkerName := make([]byte, s.validator.maxIDLengthLimit+1)
	for i := range invalidMarkerName {
		invalidMarkerName[i] = 'a'
	}
	s.Error(s.validator.validateMarkerAttributes("test-namespace", string(invalidMarkerName), validDetails))
}

func (s *commandAttrValidatorSuite) TestValidateCrossNamespaceCall() {
	testCases := []struct {
		name                string
		namespace           string
		targetNamespace     string
		allowCrossNamespace bool
		expectError         bool
	}{
		{
			name:                "same namespace is allowed",
			namespace:           "test-namespace",
			targetNamespace:     "test-namespace",
			allowCrossNamespace: false,
			expectError:         false,
		},
		{
			name:                "different namespace with cross namespace disabled",
			namespace:           "test-namespace",
			targetNamespace:     "different-namespace",
			allowCrossNamespace: false,
			expectError:         true,
		},
		{
			name:                "different namespace with cross namespace enabled",
			namespace:           "test-namespace",
			targetNamespace:     "different-namespace",
			allowCrossNamespace: true,
			expectError:         false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Setup mock expectations
			if tc.namespace != tc.targetNamespace {
				nsEntry := namespace.NewNamespaceForTest(
					nil,
					&persistencespb.NamespaceConfig{},
					false,
					&persistencespb.NamespaceReplicationConfig{},
					0,
				)
				s.mockNamespaceRegistry.EXPECT().GetNamespace(namespace.Name(tc.namespace)).Return(nsEntry, nil).AnyTimes()
				s.mockNamespaceRegistry.EXPECT().GetNamespace(namespace.Name(tc.targetNamespace)).Return(nsEntry, nil).AnyTimes()
			}

			// Create a new validator with the specific cross-namespace setting
			cfg := tests.NewDynamicConfig()
			cfg.EnableCrossNamespaceCommands = dynamicconfig.GetBoolPropertyFn(tc.allowCrossNamespace)
			validator := NewCommandAttrValidator(
				s.mockNamespaceRegistry,
				cfg,
				searchattribute.NewValidator(
					searchattribute.NewTestProvider(),
					s.mockVisibilityManager,
					dynamicconfig.GetIntPropertyFnFilteredByNamespace(256),
					dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
					false,
				),
			)

			err := validator.validateCrossNamespaceCall(tc.namespace, tc.targetNamespace)
			if tc.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_UnknownCommandType() {
	unknownCommand := &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_UNSPECIFIED,
	}
	err := s.validator.ValidateCommandSequence([]*commandpb.Command{unknownCommand})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Contains(err.Error(), "unknown command type")
}

func (s *commandAttrValidatorSuite) TestTruncatedCommandTypes_Empty() {
	result := s.validator.truncatedCommandTypes([]*commandpb.Command{})
	s.Equal("", result)
}

func (s *commandAttrValidatorSuite) TestTruncatedCommandTypes_SingleLongType() {
	// Create a command with a very long type name that exceeds the limit by itself
	// Since COMMAND_TYPE_UNSPECIFIED is the shortest, use a custom string via proto
	// but we can't easily do that. Instead, we'll test with many commands.
	var commands []*commandpb.Command
	for i := 0; i < 300; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		})
	}
	result := s.validator.truncatedCommandTypes(commands)
	s.LessOrEqual(len(result), maxCommandSequenceErrorLength)
	s.Contains(result, "...")
}

func (s *commandAttrValidatorSuite) TestTruncatedCommandTypes_Boundary() {
	// Test exactly at the boundary: create commands that would produce a string
	// just under the limit, then add one more to trigger truncation.
	// "SCHEDULE_ACTIVITY_TASK" is 22 chars, plus ", " = 24 chars.
	// 2048 / 24 = 85.3, so 85 commands = 2040 chars, 86 = 2064 chars.
	var commands []*commandpb.Command
	for i := 0; i < 86; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		})
	}
	result := s.validator.truncatedCommandTypes(commands)
	s.LessOrEqual(len(result), maxCommandSequenceErrorLength)
	s.Contains(result, "...")
}

func (s *commandAttrValidatorSuite) TestValidateCommandSequence_LargeCommandList() {
	// Simulate a real-world scenario: a workflow with many RecordMarker commands
	// followed by a CompleteWorkflowExecution, then more commands.
	var commands []*commandpb.Command
	for i := 0; i < 500; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		})
	}
	commands = append(commands, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
	})
	for i := 0; i < 100; i++ {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		})
	}

	err := s.validator.ValidateCommandSequence(commands)
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)

	msg := err.Error()
	s.LessOrEqual(len(msg), maxCommandSequenceErrorLength+150,
		"error message should be bounded even with 600+ commands")
	s.Contains(msg, "...")
}

func TestCommandTypes(t *testing.T) {
	validator := NewCommandAttrValidator(
		namespace.NewMockRegistry(gomock.NewController(t)),
		tests.NewDynamicConfig(),
		searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			manager.NewMockVisibilityManager(gomock.NewController(t)),
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(256),
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
			false,
		),
	)

	commands := []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
	}
	result := validator.commandTypes(commands)
	assert.Equal(t, []string{"SCHEDULE_ACTIVITY_TASK", "RECORD_MARKER"}, result)
}

func TestTruncatedCommandTypes(t *testing.T) {
	validator := NewCommandAttrValidator(
		namespace.NewMockRegistry(gomock.NewController(t)),
		tests.NewDynamicConfig(),
		searchattribute.NewValidator(
			searchattribute.NewTestProvider(),
			manager.NewMockVisibilityManager(gomock.NewController(t)),
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(256),
			dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
			false,
		),
	)

	// Test no truncation needed
	shortCommands := []*commandpb.Command{
		{CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK},
		{CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER},
	}
	result := validator.truncatedCommandTypes(shortCommands)
	assert.Equal(t, "SCHEDULE_ACTIVITY_TASK, RECORD_MARKER", result)

	// Test truncation
	var longCommands []*commandpb.Command
	for i := 0; i < 200; i++ {
		longCommands = append(longCommands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
		})
	}
	result = validator.truncatedCommandTypes(longCommands)
	assert.LessOrEqual(t, len(result), maxCommandSequenceErrorLength)
	assert.Contains(t, result, "...")
}
