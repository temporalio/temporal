package workflow_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/components/nexusoperations"
	opsworkflow "go.temporal.io/server/components/nexusoperations/workflow"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type commandValidator struct {
	maxPayloadSize int
}

func (v commandValidator) IsValidPayloadSize(size int) bool {
	return size <= v.maxPayloadSize
}

type testContext struct {
	execInfo        *persistencespb.WorkflowExecutionInfo
	ms              *historyi.MockMutableState
	scheduleHandler workflow.CommandHandler
	cancelHandler   workflow.CommandHandler
	history         *historypb.History
}

var defaultConfig = &nexusoperations.Config{
	Enabled:                            dynamicconfig.GetBoolPropertyFn(true),
	MaxServiceNameLength:               dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("service")),
	MaxOperationNameLength:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(len("op")),
	MaxConcurrentOperations:            dynamicconfig.GetIntPropertyFnFilteredByNamespace(2),
	MaxOperationHeaderSize:             dynamicconfig.GetIntPropertyFnFilteredByNamespace(20),
	DisallowedOperationHeaders:         dynamicconfig.GetTypedPropertyFn([]string{"request-timeout"}),
	MaxOperationScheduleToCloseTimeout: dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Hour * 24),
}

func newTestContext(t *testing.T, cfg *nexusoperations.Config) testContext {
	endpointReg := nexustest.FakeEndpointRegistry{
		OnGetByName: func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
			if endpointName == "endpoint caller namespace unauthorized" {
				return nil, serviceerror.NewPermissionDenied("caller namespace unauthorized", "")
			} else if endpointName != "endpoint" {
				return nil, serviceerror.NewNotFound("endpoint not found")
			}
			// Only the ID is taken here.
			return persistencespb.NexusEndpointEntry_builder{Id: "endpoint-id"}.Build(), nil
		},
	}
	chReg := workflow.NewCommandHandlerRegistry()
	require.NoError(t, opsworkflow.RegisterCommandHandlers(chReg, endpointReg, cfg))
	smReg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(smReg))
	require.NoError(t, nexusoperations.RegisterStateMachines(smReg))
	require.NoError(t, nexusoperations.RegisterEventDefinitions(smReg))
	ms := historyi.NewMockMutableState(gomock.NewController(t))
	node, err := hsm.NewRoot(smReg, workflow.StateMachineType, ms, make(map[string]*persistencespb.StateMachineMap), ms)
	require.NoError(t, err)
	ms.EXPECT().IsTransitionHistoryEnabled().Return(false).AnyTimes()
	ms.EXPECT().HSM().Return(node).AnyTimes()
	lastEventID := int64(4)
	history := &historypb.History{}
	ms.EXPECT().AddHistoryEvent(gomock.Any(), gomock.Any()).DoAndReturn(func(t enumspb.EventType, setAttrs func(he *historypb.HistoryEvent)) *historypb.HistoryEvent {
		e := historypb.HistoryEvent_builder{
			Version:   1,
			EventId:   lastEventID,
			EventTime: timestamppb.Now(),
		}.Build()
		lastEventID++
		setAttrs(e)
		history.SetEvents(append(history.GetEvents(), e))
		return e
	}).AnyTimes()

	execInfo := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()
	scheduleHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION)
	require.True(t, ok)
	cancelHandler, ok := chReg.Handler(enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION)
	require.True(t, ok)

	return testContext{
		execInfo:        execInfo,
		ms:              ms,
		history:         history,
		scheduleHandler: scheduleHandler,
		cancelHandler:   cancelHandler,
	}
}

func TestHandleScheduleCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("endpoint not found - rejected by config", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "not found",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("caller namespace unauthorized", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint caller namespace unauthorized",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("exceeds max service length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "too long",
				Operation: "op",
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("exceeds max operation length", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "too long",
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("exceeds max operation header size", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
				NexusHeader: map[string]string{
					"key1234567890": "value1234567890",
				},
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("invalid header keys", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
				NexusHeader: map[string]string{
					"request-timeout": "1s",
				},
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("exceeds max payload size", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
				Input: commonpb.Payload_builder{
					Data: []byte("ab"),
				}.Build(),
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.True(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("exceeds max concurrent operations", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		for i := 0; i < 2; i++ {
			err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
				ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
					Endpoint:  "endpoint",
					Service:   "service",
					Operation: "op",
				}.Build(),
			}.Build())
			require.NoError(t, err)
		}
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_NEXUS_OPERATIONS_LIMIT_EXCEEDED, failWFTErr.Cause)
		require.Equal(t, 2, len(tcx.history.GetEvents()))
	})

	t.Run("schedule to close timeout capped by run timeout", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.execInfo.SetWorkflowRunTimeout(durationpb.New(time.Hour))
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:               "endpoint",
				Service:                "service",
				Operation:              "op",
				ScheduleToCloseTimeout: durationpb.New(time.Hour * 2),
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		require.Equal(t, time.Hour, tcx.history.GetEvents()[0].GetNexusOperationScheduledEventAttributes().GetScheduleToCloseTimeout().AsDuration())
	})

	t.Run("schedule to close timeout capped by dynamic config", func(t *testing.T) {
		cfg := *defaultConfig
		cfg.MaxOperationScheduleToCloseTimeout = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Minute)
		tcx := newTestContext(t, &cfg)
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:               "endpoint",
				Service:                "service",
				Operation:              "op",
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		require.Equal(t, time.Minute, tcx.history.GetEvents()[0].GetNexusOperationScheduledEventAttributes().GetScheduleToCloseTimeout().AsDuration())
	})

	timeoutCases := []struct {
		name               string
		workflowRunTimeout *durationpb.Duration
		commandTimeout     *durationpb.Duration
		expectedTimeout    *durationpb.Duration
	}{
		{
			name:               "operation timeout defaults to workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     nil,
			expectedTimeout:    durationpb.New(time.Minute),
		},
		{
			name:               "operation timeout trimmed to workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     durationpb.New(time.Hour),
			expectedTimeout:    durationpb.New(time.Minute),
		},
		{
			name:               "operation timeout left as is if less than workflow run timeout",
			workflowRunTimeout: durationpb.New(time.Minute),
			commandTimeout:     durationpb.New(time.Second),
			expectedTimeout:    durationpb.New(time.Second),
		},
		{
			name:               "operation timeout left as is if no workflow run timeout",
			workflowRunTimeout: nil,
			commandTimeout:     durationpb.New(time.Second),
			expectedTimeout:    durationpb.New(time.Second),
		},
	}
	for _, tc := range timeoutCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)

			tcx.ms.GetExecutionInfo().SetWorkflowRunTimeout(tc.workflowRunTimeout)
			err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
				ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
					Endpoint:               "endpoint",
					Service:                "service",
					Operation:              "op",
					ScheduleToCloseTimeout: tc.commandTimeout,
				}.Build(),
			}.Build())
			require.NoError(t, err)
			require.Equal(t, 1, len(tcx.history.GetEvents()))
			require.Equal(t, tc.expectedTimeout.AsDuration(), tcx.history.GetEvents()[0].GetNexusOperationScheduledEventAttributes().GetScheduleToCloseTimeout().AsDuration())
		})
	}

	t.Run("sets event attributes with UserMetadata and spawns a child operation machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		cAttrs := commandpb.ScheduleNexusOperationCommandAttributes_builder{
			Endpoint:  "endpoint",
			Service:   "service",
			Operation: "op",
			Input:     &commonpb.Payload{},
			NexusHeader: map[string]string{
				"key": "value",
			},
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
		}.Build()
		userMetadata := sdkpb.UserMetadata_builder{
			Summary: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			}.Build(),
			Details: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			}.Build(),
		}.Build()
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: proto.ValueOrDefault(cAttrs),
			UserMetadata:                            userMetadata,
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		event := tcx.history.GetEvents()[0]
		eAttrs := event.GetNexusOperationScheduledEventAttributes()
		require.Equal(t, cAttrs.GetService(), eAttrs.GetService())
		require.Equal(t, cAttrs.GetOperation(), eAttrs.GetOperation())
		require.Equal(t, cAttrs.GetInput(), eAttrs.GetInput())
		require.Equal(t, cAttrs.GetScheduleToCloseTimeout(), eAttrs.GetScheduleToCloseTimeout())
		require.Equal(t, cAttrs.GetNexusHeader(), eAttrs.GetNexusHeader())
		require.Equal(t, int64(1), eAttrs.GetWorkflowTaskCompletedEventId())
		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType, ID: strconv.FormatInt(event.GetEventId(), 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)
		require.EqualExportedValues(t, userMetadata, event.GetUserMetadata())
	})
}

func TestHandleCancelCommand(t *testing.T) {
	t.Run("feature disabled", func(t *testing.T) {
		tcx := newTestContext(t, &nexusoperations.Config{
			Enabled: dynamicconfig.GetBoolPropertyFn(false),
		})
		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_FEATURE_DISABLED, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("empty attributes", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, &commandpb.Command{})
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("operation not found", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(false)

		err := tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			RequestCancelNexusOperationCommandAttributes: commandpb.RequestCancelNexusOperationCommandAttributes_builder{
				ScheduledEventId: 5,
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 0, len(tcx.history.GetEvents()))
	})

	t.Run("operation already completed", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(false)

		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		event := tcx.history.GetEvents()[0]

		// Complete the operation using CompletedEventDefinition to ensure proper deletion
		err = nexusoperations.CompletedEventDefinition{}.Apply(tcx.ms.HSM(), historypb.HistoryEvent_builder{
			EventId: event.GetEventId() + 1,
			NexusOperationCompletedEventAttributes: historypb.NexusOperationCompletedEventAttributes_builder{
				ScheduledEventId: event.GetEventId(),
			}.Build(),
		}.Build())
		require.NoError(t, err)

		// Try to cancel - should fail since operation is deleted
		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			RequestCancelNexusOperationCommandAttributes: commandpb.RequestCancelNexusOperationCommandAttributes_builder{
				ScheduledEventId: event.GetEventId(),
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.False(t, failWFTErr.TerminateWorkflow)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Equal(t, 1, len(tcx.history.GetEvents())) // Only scheduled event should be recorded.
	})

	t.Run("operation already completed - completion buffered", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(true).AnyTimes()

		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		event := tcx.history.GetEvents()[0]

		err = nexusoperations.CompletedEventDefinition{}.Apply(tcx.ms.HSM(), historypb.HistoryEvent_builder{
			EventId: event.GetEventId() + 1,
			NexusOperationCompletedEventAttributes: historypb.NexusOperationCompletedEventAttributes_builder{
				ScheduledEventId: event.GetEventId(),
			}.Build(),
		}.Build())
		require.NoError(t, err)

		// Try to cancel - should succeed because there's a buffered completion
		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			RequestCancelNexusOperationCommandAttributes: commandpb.RequestCancelNexusOperationCommandAttributes_builder{
				ScheduledEventId: event.GetEventId(),
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, 2, len(tcx.history.GetEvents())) // Both scheduled and cancel requested events should be recorded
		crAttrs := tcx.history.GetEvents()[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.GetEventId(), crAttrs.GetScheduledEventId())
	})

	t.Run("sets event attributes with UserMetadata and spawns cancelation child machine", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(false).AnyTimes()
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		userMetadata := sdkpb.UserMetadata_builder{
			Summary: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test summary Data`),
			}.Build(),
			Details: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
				Data:     []byte(`Test Details Data`),
			}.Build(),
		}.Build()
		require.NoError(t, err)
		require.Equal(t, 1, len(tcx.history.GetEvents()))
		event := tcx.history.GetEvents()[0]

		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 1, commandpb.Command_builder{
			RequestCancelNexusOperationCommandAttributes: commandpb.RequestCancelNexusOperationCommandAttributes_builder{
				ScheduledEventId: event.GetEventId(),
			}.Build(),
			UserMetadata: userMetadata,
		}.Build())
		require.NoError(t, err)

		child, err := tcx.ms.HSM().Child([]hsm.Key{{Type: nexusoperations.OperationMachineType, ID: strconv.FormatInt(event.GetEventId(), 10)}})
		require.NoError(t, err)
		require.NotNil(t, child)

		require.Equal(t, 2, len(tcx.history.GetEvents()))
		crAttrs := tcx.history.GetEvents()[1].GetNexusOperationCancelRequestedEventAttributes()
		require.Equal(t, event.GetEventId(), crAttrs.GetScheduledEventId())
		require.Equal(t, int64(1), crAttrs.GetWorkflowTaskCompletedEventId())
		savedUserMetadata := tcx.history.GetEvents()[1].GetUserMetadata()
		require.EqualExportedValues(t, userMetadata, savedUserMetadata)

		child, err = child.Child([]hsm.Key{nexusoperations.CancelationMachineKey})
		require.NoError(t, err)
		require.NotNil(t, child)
		userMetadata = nil
	})
}

func TestOperationNodeDeletionOnTerminalEvents(t *testing.T) {
	scheduleOperation := func(t *testing.T, tcx testContext) (scheduledEvent *historypb.HistoryEvent, nodeID string) {
		err := tcx.scheduleHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 100}, 1, commandpb.Command_builder{
			ScheduleNexusOperationCommandAttributes: commandpb.ScheduleNexusOperationCommandAttributes_builder{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Len(t, tcx.history.GetEvents(), 1)
		scheduledEvent = tcx.history.GetEvents()[0]

		coll := nexusoperations.MachineCollection(tcx.ms.HSM())
		nodeID = strconv.FormatInt(scheduledEvent.GetEventId(), 10)
		_, err = coll.Node(nodeID)
		require.NoError(t, err)
		return
	}

	applyTerminalEventAndAssertDeletion := func(
		t *testing.T,
		tcx testContext,
		scheduledEventID int64,
		eventType enumspb.EventType,
		eventAttr interface{},
		def hsm.EventDefinition,
	) {
		coll := nexusoperations.MachineCollection(tcx.ms.HSM())
		nodeID := strconv.FormatInt(scheduledEventID, 10)

		event := historypb.HistoryEvent_builder{
			Version:   1,
			EventId:   scheduledEventID + 1,
			EventType: eventType,
			EventTime: timestamppb.Now(),
		}.Build()

		switch eventType { //nolint:exhaustive
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
			event.SetNexusOperationCompletedEventAttributes(proto.ValueOrDefault(eventAttr.(*historypb.NexusOperationCompletedEventAttributes)))
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
			event.SetNexusOperationFailedEventAttributes(proto.ValueOrDefault(eventAttr.(*historypb.NexusOperationFailedEventAttributes)))
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
			event.SetNexusOperationCanceledEventAttributes(proto.ValueOrDefault(eventAttr.(*historypb.NexusOperationCanceledEventAttributes)))
		case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
			event.SetNexusOperationTimedOutEventAttributes(proto.ValueOrDefault(eventAttr.(*historypb.NexusOperationTimedOutEventAttributes)))
		default:
			panic(fmt.Sprintf("unexpected event type in test: %v", eventType))
		}

		require.NoError(t, def.Apply(tcx.ms.HSM(), event))

		_, err := coll.Node(nodeID)
		require.Error(t, err, "node should be deleted after terminal event")

		tcx.ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).Return(false)

		err = tcx.cancelHandler(context.Background(), tcx.ms, commandValidator{maxPayloadSize: 1}, 2, commandpb.Command_builder{
			RequestCancelNexusOperationCommandAttributes: commandpb.RequestCancelNexusOperationCommandAttributes_builder{
				ScheduledEventId: scheduledEventID,
			}.Build(),
		}.Build())
		var failWFTErr workflow.FailWorkflowTaskError
		require.ErrorAs(t, err, &failWFTErr)
		require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_NEXUS_OPERATION_ATTRIBUTES, failWFTErr.Cause)
		require.Len(t, tcx.history.GetEvents(), 1, "no new events after attempting to cancel a terminated operation")
	}

	cases := []struct {
		name      string
		eventType enumspb.EventType
		eventAttr interface{}
		eventDef  hsm.EventDefinition
	}{
		{
			name:      "completed event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
			eventAttr: &historypb.NexusOperationCompletedEventAttributes{},
			eventDef:  nexusoperations.CompletedEventDefinition{},
		},
		{
			name:      "failed event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
			eventAttr: &historypb.NexusOperationFailedEventAttributes{},
			eventDef:  nexusoperations.FailedEventDefinition{},
		},
		{
			name:      "canceled event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
			eventAttr: &historypb.NexusOperationCanceledEventAttributes{},
			eventDef:  nexusoperations.CanceledEventDefinition{},
		},
		{
			name:      "timed out event deletes node",
			eventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
			eventAttr: &historypb.NexusOperationTimedOutEventAttributes{},
			eventDef:  nexusoperations.TimedOutEventDefinition{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)
			scheduledEvent, _ := scheduleOperation(t, tcx)

			switch a := tc.eventAttr.(type) {
			case *historypb.NexusOperationCompletedEventAttributes:
				a.SetScheduledEventId(scheduledEvent.GetEventId())
			case *historypb.NexusOperationFailedEventAttributes:
				a.SetScheduledEventId(scheduledEvent.GetEventId())
			case *historypb.NexusOperationCanceledEventAttributes:
				a.SetScheduledEventId(scheduledEvent.GetEventId())
			case *historypb.NexusOperationTimedOutEventAttributes:
				a.SetScheduledEventId(scheduledEvent.GetEventId())
			}

			applyTerminalEventAndAssertDeletion(t, tcx, scheduledEvent.GetEventId(), tc.eventType, tc.eventAttr, tc.eventDef)
		})
	}
}
